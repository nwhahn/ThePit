"""
Small script that takes the iex csv output and dumps to the database
"""

from argparse import ArgumentParser

import pandas as pd

from lib.alerting import get_alerter
from lib.logger import get_logger, log_on_failure
from lib.database import Database, DbInfo, get_columns

__app__ = "iex_data_inserter"
logger = get_logger(__name__, __app__)
alerter = get_alerter()


def inst_df(db_inf: DbInfo, message: str, path: str) -> None:
    df = pd.read_csv(f"{path}/iex_{message}.csv", index_col=0, sep="|")
    df.columns = map(str.lower, df.columns)
    logger.info(df.columns)

    cols = get_columns(db_inf)
    logger.info(f"Writing out columns from db in order: {cols}")

    df = df[cols]

    db_inf.database.copy(df, db_inf.schema, db_inf.table)

    alerter.info(f"Inserted {len(df)} rows into {db_inf.schema}.{db_inf.table}")


@log_on_failure
def main():
    parser = ArgumentParser(description="Dump csvs into the database")
    parser.add_argument('--path', help='path to the csv file', required=True)
    parser.add_argument('--message', help='message type to use', required=True)
    parser.add_argument('--schema', help='schema to insert into', required=True)
    parser.add_argument('--table', help='table to insert into', required=True)
    parser.add_argument('--db-acc', help='database account', required=True)
    args = parser.parse_args()

    db_inf = DbInfo(Database(args.db_acc), args.schema, args.table)

    inst_df(db_inf, args.message, args.path)


if __name__ == '__main__':
    main()
    alerter.send_message(__app__)
