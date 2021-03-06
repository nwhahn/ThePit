"""
Small script that takes the iex csv output and dumps to the database
"""

from argparse import ArgumentParser
import datetime as dt

import pandas as pd

from lib.alerting import get_alerter
from lib.logger import get_logger, app_main
from lib.database import Database, DbInfo, get_columns
import lib.config_parser as config_parser

__app__ = "iex_data_inserter"
logger = get_logger(__app__)
alerter = get_alerter(__app__)


def initify_cols(df: pd.DataFrame, db_inf: DbInfo) -> pd.DataFrame:
    query = f"SELECT column_name, data_type from information_schema.columns where table_schema = '{db_inf.schema}' " \
            f"and table_name = '{db_inf.table}'"
    cols = db_inf.database.execute(query)

    for col in cols:
        if col['data_type'] in ('smallint', 'integer', 'bigint'):  # postgres int types
            logger.info(f"Filling {col} nulls with zeros and converting to int")
            df[col['column_name']] = df[col['column_name']].fillna(0)
            df[col['column_name']] = df[col['column_name']].astype(int)

    return df


def dateify(df: pd.DataFrame, date_val: str) -> pd.DataFrame:
    some_date = 0
    for date_name in ('date', 'tradedate'):
        if date_name in df.columns.values:
            some_date += 1

    if not some_date:
        df['date'] = date_val

    return df


def inst_df(config: config_parser.ConfigNode) -> None:
    alerter.info(f"Uploading for message: {config['iex_uploader.message']}")
    logger.info(f"Uploading for message: {config['iex_uploader.message']}")

    db_inf = DbInfo(Database(config, config['iex_uploader.db_acc']), config['iex_uploader.schema'],
                    config['iex_uploader.table'])

    df = pd.read_csv(f"{config['iex_uploader.path']}/iex_{config['iex_uploader.message']}.csv", sep="|", index_col=0)
    df.columns = map(str.lower, df.columns)
    logger.info(df.columns)

    cols = get_columns(db_inf)
    logger.info(cols)
    logger.info(sorted(list(df.columns)))

    logger.info(df.columns)

    df = initify_cols(df, db_inf)

    df = dateify(df, config['args.date'])

    df = df[cols]

    db_inf.database.copy(df, db_inf.schema, db_inf.table)

    alerter.info(f"Inserted {len(df)} rows into {db_inf.schema}.{db_inf.table}")


@app_main(logger, alerter)
def main():
    today = dt.date.today()
    parser = ArgumentParser(description="Dump csvs into the database")
    parser.add_argument('--date', help='date as a string if there isnt a date already in the columns', default=today)
    config = config_parser.config_argparse(parser)

    inst_df(config)


if __name__ == '__main__':
    main()
