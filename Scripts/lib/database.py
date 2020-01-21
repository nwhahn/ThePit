import logging
import io
from typing import List

import pandas as pd
import psycopg2
import psycopg2.extras

from lib.config_parser import ConfigNode


class DbInfo:
    def __init__(self, database, schema, table):
        self.database = database
        self.schema = schema
        self.table = table


class Database:
    def __init__(self, config: ConfigNode, account: str):
        print(config['database']['bitcoin_writer'])
        self.db_config = config['database'][account]

    def execute(self, sql: str) -> list:
        """This function is not good for large queries"""
        logging.info(sql)

        with psycopg2.connect(host=self.db_config['host'],
                              user=self.db_config['user'],
                              password=self.db_config['password'],
                              dbname=self.db_config['database'],
                              cursor_factory=psycopg2.extras.RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                if 'select' in cursor.statusmessage.lower():
                    output = cursor.fetchall()
                else:
                    output = None
        if connection:
            connection.close()

        return output

    def copy(self, df: pd.DataFrame, schema: str, table: str, *args, **kwargs) -> None:
        with psycopg2.connect(host=self.db_config['host'],
                              user=self.db_config['user'],
                              password=self.db_config['password'],
                              dbname=self.db_config['database']) as connection:
            with connection.cursor() as cursor:
                s_buf = io.StringIO()
                logging.info(df)
                df.to_csv(s_buf, sep='|', header=False, index=False, *args, **kwargs)
                s_buf.seek(0)
                cursor.copy_from(s_buf, f'{schema}.{table}', sep='|', null='')

        if connection:
            connection.close()

    def get_df(self, sql: str) -> pd.DataFrame:
        return pd.DataFrame(self.execute(sql))


def create_connection(config: ConfigNode, account: str):
    """factor function just in case"""
    return Database(config, account)


def get_columns(db_inf: DbInfo) -> List[str]:
    query = f"SELECT column_name from information_schema.columns where table_schema = '{db_inf.schema}' and " \
            f"table_name = '{db_inf.table}'"
    logging.info(query)
    return [r['column_name'] for r in db_inf.database.execute(query)]


if __name__ == '__main__':
    a = Database('bitcoin_writer')
    b = pd.read_csv('/home/jackh/google_trends_BITCOINKEYWORDS_2019_11_21.csv.0', index_col=0, sep='|')
    # a.copy(b, "bitcoin", "bitcoinkeywords")