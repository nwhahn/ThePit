"""
Script to download an iex message and create a csv from it

please not it requires there to be a symbol at the moment
"""

from argparse import ArgumentParser
from typing import List, Tuple

import requests
import pandas as pd

import lib.database as database
from lib.logger import get_logger, app_main
from lib.alerting import get_alerter
from lib.fs import make_symlink
import lib.config_parser as config_parser

__app__ = "iex_downloader"
logger = get_logger(__app__)
alerter = get_alerter(__app__)

IEX_REQUEST = 'https://cloud.iexapis.com/stable/stock/market/batch?symbols={}&types={}&token={}'


def db_syms(db_inf: database.DbInfo, table: str) -> dict:
    """return dictionary of symbol to symbolid mapping"""
    output = db_inf.database.execute(f"select symbolid, nasdaqsymbol as symbol from {db_inf.schema}.{table} "
                                     f"where nasdaqsymbol is not null")
    return {row['symbol']: row['symbolid'] for row in output}


def dry_run_map(syms: List[str], db_inf: database.DbInfo, table: str) -> dict:
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


def ohlc_request(syms: List[str], token: str, message: str, date_range: str = None) -> Tuple[List[dict], List[str]]:

    logger.info(syms)
    symbols = ','.join(syms)
    if date_range is None:
        url = requests.get(IEX_REQUEST.format(symbols, message, token))
    else:
        range_str = f"{IEX_REQUEST.format(symbols, message, token)}&range={date_range}"
        url = requests.get(range_str)
    logger.info(f"{url.url}")
    logger.info(f"status code: {url.status_code}")

    if url.status_code != 200:
        return [{} for _ in syms], syms
    sym_list = []
    iex_json = url.json()

    for k, v in iex_json.items():
        temp = v[message]
        if temp is not None:
            if isinstance(temp, list):
                for t in temp:
                    t['symbol'] = k
                    sym_list.append(t)
            else:
                temp['symbol'] = k
                sym_list.append(temp)

    missing_syms = set(syms) - set([r['symbol'] for r in sym_list])

    return sym_list, list(missing_syms)


def iex_ohlc(config: config_parser.ConfigNode):

    db_inf = database.DbInfo(database.create_connection(config, config['iex_messaging.db_acc']),
                             config['iex_messaging.schema'], config['iex_messaging.table'])

    ref_table = config['iex_messaging.ref_table']

    if 'args.symbols' not in config:
        syms = db_syms(db_inf, ref_table)
    else:
        arg_syms = config['args.symbols'].replace(' ', '').split(',')
        syms = dry_run_map(arg_syms, db_inf, ref_table)

    jobs = gen_jobs(syms, config['iex_messaging.syms_per_job'])

    total_sym_list, failed_syms = [], []
    for j in jobs:
        sym_list, missing_syms = ohlc_request(j, config['args.token'], config['iex_messaging.message'],
                                              config['args.range'])
        total_sym_list.extend(sym_list)
        failed_syms.extend(missing_syms)

    df = pd.DataFrame(total_sym_list)
    df['symbolid'] = df['symbol'].map(syms)

    make_symlink(df, config['iex_messaging.outpath'], f'iex_{config["iex_messaging.message"]}', '|')

    logger.info(f"All missing symbols: {failed_syms}")

    alerter.info(f"Number of symbols gathered: {len(total_sym_list)}")
    alerter.info(f"Number of missing symbols: {len(failed_syms)}")


@app_main(logger)
def main():
    # TODO add yaml support and config, this is too many argparse variables
    parser = ArgumentParser(description="Download ohlc for yesterday")
    parser.add_argument('--token', help='iex token, dont save to git ;)', required=True)
    parser.add_argument('--symbols', help='use specific symbols possibly to test')
    parser.add_argument('--range', help='specify a range for things like looking up dividends')
    config = config_parser.config_argparse(parser)

    iex_ohlc(config)


if __name__ == '__main__':
    main()
