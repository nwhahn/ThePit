import datetime as dt
import glob
import lib.logger as logger
import os
from argparse import ArgumentParser
import lib.alerting as alerting

import pandas as pd

__app__ = 'symbols_download'
logger = logger.get_logger(__name__, __app__)
alert = alerting.get_alerter()

arca_location = 'ftp://ftp.nyxdata.com/ARCASymbolMapping/ARCASymbolMapping.txt'
nyse_location = 'ftp://ftp.nyxdata.com/NYSESymbolMapping/NYSESymbolMapping.txt'
nas_ls_location = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
nas_tr_location = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqtraded.txt'


def get_version(path: str, file_name: str) -> int:
    files = glob.glob(f'{path}/{file_name}.*')
    logger.info(files)
    if len(files) == 0:
        return 0
    else:
        return max([int(v.split('.')[-1]) for v in files]) + 1


def make_symlink(df: pd.DataFrame, path: str, sym_name: str, sep: str) -> None:
    today = dt.datetime.today().strftime('%Y_%m_%d')
    creation_file = f'{sym_name}_{today}.csv'
    logger.info(f'Generating file with name: {creation_file}')
    version = get_version(path, creation_file)

    true_path = f'{path}/{creation_file}.{version}'
    logger.info(f'Writing out csv to {true_path}')
    df.to_csv(true_path, sep=sep)

    symlink_path = f'{path}/{sym_name}.csv'
    if os.path.exists(symlink_path):
        logger.info(f"Unlinking {symlink_path}")
        os.unlink(symlink_path)
    os.symlink(true_path, symlink_path)


def arca_nyse_df(sep: str, f_loc: str) -> pd.DataFrame:
    columns = ['Symbol', 'CQS_Symbol', 'SymbolIndexNumber', 'NYSEMarket', 'ListedMarket', 'TickerDesignation', 'UOT',
               'PriceScaleCode', 'NYSE_SystemID', 'BBG_BSID', 'BBG_GlobalID', '_']
    df = pd.read_csv(f_loc, sep=sep, header=None)
    df.columns = columns
    df = df.drop(columns='_')
    logger.info("Dropping extra column")

    return df


def nasdaq_df(sep: str, f_loc: str) -> pd.DataFrame:
    df = pd.read_csv(f_loc, sep=sep)
    df['CQS_Symbol'] = df['CQS Symbol']
    df['CQS_Symbol'] = df['CQS_Symbol'].fillna(df['Symbol'])
    logger.info("Changed CQS Symbol to CQS_Symbol and backfilled valued")

    return df


def main_impl(args) -> int:
    if args.arca:
        logger.info('Gathering arca symbols')
        arca_df = arca_nyse_df(args.sep, arca_location)

        logger.info(f'Got dataframe of size: {arca_df.size}')
        make_symlink(arca_df, args.path, 'symbols_arca', args.sep)
        alert.info("Created arca symlink and csv")

    if args.nyse:
        logger.info('Gathering nyse symbols')
        nyse_df = arca_nyse_df(args.sep, nyse_location)

        logger.info(f'Got dataframe of size: {nyse_df.size}')
        make_symlink(nyse_df, args.path, 'symbols_nyse', args.sep)
        alert.info("Created nyse symlink and csv")

    if args.nas_tr:
        logger.info('Gathering nasdaq traded symbols')
        nas_tr_df = nasdaq_df(args.sep, nas_tr_location)[:-1]

        logger.info(f'Got dataframe of size: {nas_tr_df.size}')
        make_symlink(nas_tr_df, args.path, 'symbols_nasdaq_traded', args.sep)
        alert.info("Created nasdaq trade symlink and csv")

    if args.nas_ls:
        logger.info('Gathering nasdaq listed symbols')
        nas_ls_df = pd.read_csv(nas_ls_location, sep=args.sep)[:-1]

        logger.info(f'Got dataframe of size: {nas_ls_df.size}')
        make_symlink(nas_ls_df, args.path, 'symbols_nasdaq_listed', args.sep)
        alert.info("Created nasdaq listed symlink and csv")

    return 0


def main():
    parser = ArgumentParser(description='Script to download refdata files, will maintain symlinks')
    parser.add_argument('--arca', help='arca ftp', action='store_true')
    parser.add_argument('--nyse', help='nyse ftp', action='store_true')
    parser.add_argument('--nas-tr', help='traded ftp', action='store_true')
    parser.add_argument('--nas-ls', help='listed ftp', action='store_true')
    parser.add_argument('--path', help='path to save the file(s) to', required=True)
    parser.add_argument('--sep', help='nyse and nasdaq use |', default='|')
    args = parser.parse_args()

    try:
        main_impl(args)
    except Exception as e:
        alert.error("Something happened check log files")
        alert.error(e)

    alert.send_message(__app__)


if __name__ == '__main__':
    main()
