from argparse import ArgumentParser
from typing import List
import os

import requests
import pandas as pd

from lib.database import Database, DbInfo, get_columns
from lib.logger import get_logger, app_main
from lib.alerting import get_alerter
from lib.fs import make_symlink

__app__ = "iex_ohlc"
logger = get_logger(__name__, 'iex_ohlc')
alerter = get_alerter()

DF_MAP = {'date': 'tradedate', 'open': 'openprice', 'close': 'closeprice', 'high': 'highprice', 'low': 'lowprice',
          'uOpen': 'uopenprice', 'uClose': 'ucloseprice', 'uHigh': 'uhighprice', 'uLow': 'ulowprice',
          'changePercent': 'changepercent'}  # just being lazy, there's probably a better way to do this

IEX_REQUEST = 'https://cloud.iexapis.com/stable/stock/market/batch?symbols={}&types=previous&token={}'


def db_syms(db_inf: DbInfo, table: str) -> dict:
    """return dictionary of symbol to symbolid mapping"""
    output = db_inf.database.execute(f"select symbolid, nasdaqsymbol as symbol from {db_inf.schema}.{table} "
                                     f"where nasdaqsymbol is not null")
    return {row['symbol']: row['symbolid'] for row in output}


def dry_run_map(syms: List[str], db_inf: DbInfo, table: str) -> dict:
    symbols = ",".join(f"'{s.upper()}'" for s in syms)
    logger.info(f"Checking for symbols: {symbols}")

    query = f"select symbolid, nasdaqsymbol as symbol from {db_inf.schema}.{table} " \
            f"where nasdaqsymbol in ({symbols})"
    logger.info(query)

    output = db_inf.database.execute(query)
    return {row['symbol']: row['symbolid'] for row in output}


def gen_jobs(syms: dict, syms_per_job: int) -> List[List[str]]:
    """split out syms into a list of lists"""
    logger.info("Generating jobs")
    comb_out = []
    temp = []

    count = 1
    for sym, _ in syms.items():
        temp.append(sym)

        if count == syms_per_job:
            logger.info(temp)
            comb_out.append(temp)
            temp = []
            count = 1

        count += 1

    if len(temp) > 0:
        comb_out.append(temp)

    return comb_out


def ohlc_request(syms: List[str], token: str) -> List[dict]:
    print(syms)
    symbols = ','.join(syms)
    url = requests.get(IEX_REQUEST.format(symbols, token))
    logger.info(f"{url.url}")
    logger.info(f"status code: {url.status_code}")

    sym_dict = []
    iex_json = url.json()

    for k, v in iex_json.items():
        temp = v['previous']
        logger.info(temp)
        sym_dict.append(temp)

    return sym_dict


def insert_into_db(sym_dict: dict, db_inf: DbInfo, path) -> None:
    """create a df and copy that whole mf into the db"""
    df = pd.DataFrame.from_dict(sym_dict).transpose()
    df['symbolid'] = df.index

    df = df.rename(columns=DF_MAP)

    df = df[get_columns(db_inf)]  # setup columns

    make_symlink(df, os.path.dirname(path), os.path.basename(path), '|')

    db_inf.database.copy(df, db_inf.schema, db_inf.table)

    alerter.info(f"Inserted {len(df)} symbols into {db_inf.schema}.{db_inf.table}")


def iex_ohlc(args):
    db_inf = DbInfo(Database(args.db_acc), args.schema, args.table)

    if args.symbols is None:
        syms = db_syms(db_inf, args.ref_table)
    else:
        arg_syms = args.symbols.split(',')
        syms = dry_run_map(arg_syms, db_inf, args.ref_table)

    jobs = gen_jobs(syms, args.syms_per_job)

    total_sym_list = []
    for j in jobs:
        sym_list = ohlc_request(j, args.token)
        total_sym_list = total_sym_list + sym_list

    # failed_syms = []

    syms_comb = {}
    for sym in total_sym_list:
        if sym is None:
            # TODO keep track of what symbols didnt get added
            logger.info("Could not get symbol")
        else:
            symbol = sym['symbol']
            logger.info(symbol)
            syms_comb[syms[symbol]] = sym

    insert_into_db(syms_comb, db_inf, args.outpath)


@app_main(logger, alerter, __app__)
def main():
    parser = ArgumentParser(description="Download ohlc for yesterday")
    parser.add_argument('--token', help='iex token, dont save to git ;)', required=True)
    parser.add_argument('--syms-per-job', help='number of syms per batch lookup', type=int, default=100)
    parser.add_argument('--db-acc', help='database account', required=True)
    parser.add_argument('--schema', help='schema for the table to write to', default='stockbois')
    parser.add_argument('--table', help='table where the ohlc data is written to', required=True)
    parser.add_argument('--ref-table', help='reference table to lookup symbols for', required=True)
    parser.add_argument('--outpath', help='path to save the backup csv to', required=True)
    parser.add_argument('--symbols', help='use specific symbols possibly to test')
    args = parser.parse_args()

    iex_ohlc(args)


if __name__ == '__main__':
    main()
