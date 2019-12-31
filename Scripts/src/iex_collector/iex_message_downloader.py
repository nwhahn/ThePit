"""
Script to download an iex message and create a csv from it

please not it requires there to be a symbol at the moment
"""

from argparse import ArgumentParser
from typing import List, Tuple

import requests
import pandas as pd

from lib.database import Database, DbInfo
from lib.logger import get_logger, log_on_failure
from lib.alerting import get_alerter
from lib.fs import make_symlink

__app__ = "iex_downloader"
logger = get_logger(__name__, __app__)
alerter = get_alerter()

IEX_REQUEST = 'https://cloud.iexapis.com/stable/stock/market/batch?symbols={}&types={}&token={}'


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


def ohlc_request(syms: List[str], token: str, message: str) -> Tuple[dict, List[str]]:

    logger.info(syms)
    symbols = ','.join(syms)
    url = requests.get(IEX_REQUEST.format(symbols, message, token))
    logger.info(f"{url.url}")
    logger.info(f"status code: {url.status_code}")

    sym_dict = {}
    iex_json = url.json()

    for k, v in iex_json.items():
        temp = v[message]
        if temp is not None:
            temp['symbol'] = k
            sym_dict[k] = temp

    missing_syms = set(syms) - set(sym_dict.keys())

    return sym_dict, list(missing_syms)


def iex_ohlc(args):
    db_inf = DbInfo(Database(args.db_acc), args.schema, args.table)

    if args.symbols is None:
        syms = db_syms(db_inf, args.ref_table)
    else:
        arg_syms = args.symbols.split(',')
        syms = dry_run_map(arg_syms, db_inf, args.ref_table)

    jobs = gen_jobs(syms, args.syms_per_job)

    total_sym_dict = {}
    failed_syms = []
    for j in jobs:
        sym_dict, missing_syms = ohlc_request(j, args.token, args.message)
        total_sym_dict.update(sym_dict)
        failed_syms.extend(missing_syms)

    df = pd.DataFrame.from_dict(total_sym_dict).transpose()
    df['symbolid'] = df['symbol'].map(syms)

    make_symlink(df, args.outpath, f'iex_{args.message}', '|')

    logger.info(f"All missing symbols: {failed_syms}")

    alerter.info(f"Number of symbols gathered: {len(total_sym_dict)}")
    alerter.info(f"Number of missing symbols: {len(failed_syms)}")


@log_on_failure
def main():
    # TODO add yaml support and config, this is too many argparse variables
    parser = ArgumentParser(description="Download ohlc for yesterday")
    parser.add_argument('--token', help='iex token, dont save to git ;)', required=True)
    parser.add_argument('--syms-per-job', help='number of syms per batch lookup', type=int, default=100)
    parser.add_argument('--db-acc', help='database account', required=True)
    parser.add_argument('--schema', help='schema for the table to write to', default='stockbois')
    parser.add_argument('--table', help='table where the ohlc data is written to', required=True)
    parser.add_argument('--ref-table', help='reference table to lookup symbols for', required=True)
    parser.add_argument('--outpath', help='path to save the backup csv to', required=True)
    parser.add_argument('--symbols', help='use specific symbols possibly to test')
    parser.add_argument('--message', help='specific message to download')
    args = parser.parse_args()

    iex_ohlc(args)


if __name__ == '__main__':
    main()
    # alerter.send_message(__app__)
