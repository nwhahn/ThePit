import psycopg2
import psycopg2.extras
import configparser
import logging
import os
import pandas as pd
import io


class DbInfo:
    def __init__(self, database, schema, table):
        self.database = database
        self.schema = schema
        self.table = table


class Database:
    def __init__(self, account, config_file=f'{os.path.dirname(os.path.realpath(__file__))}/config/Database.cfg'):
        config = configparser.ConfigParser()
        config.read(config_file)
        acc = account.lower()
        logging.info(config[acc])
        self.db_config = config[acc]

    def execute(self, sql: str) -> list:
        """This function is not good for large queries"""
        logging.info(sql)

        with psycopg2.connect(host=self.db_config['HOST'],
                              user=self.db_config['USER'],
                              password=self.db_config['PASSWORD'],
                              dbname=self.db_config['DATABASE'],
                              cursor_factory=psycopg2.extras.RealDictCursor) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                output = cursor.fetchall()
        if connection:
            connection.close()

        return output

    def copy(self, df: pd.DataFrame, schema: str, table: str) -> None:
        with psycopg2.connect(host=self.db_config['HOST'],
                              user=self.db_config['USER'],
                              password=self.db_config['PASSWORD'],
                              dbname=self.db_config['DATABASE']) as connection:
            with connection.cursor() as cursor:
                s_buf = io.StringIO()
                df.to_csv(s_buf, header=False, index=False)
                s_buf.seek(0)
                cursor.copy_from(s_buf, f'{schema}.{table}', sep=',')

        if connection:
            connection.close()

    def get_df(self, sql: str) -> pd.DataFrame:
        return pd.DataFrame(self.execute(sql))


def get_default():
    return Database('DATABASE_READER')


if __name__ == '__main__':
    a = Database('bitcoin_writer')
    b = pd.read_csv('/home/jackh/google_trends_BITCOINKEYWORDS_2019_11_21.csv.0', index_col=0, sep='|')
    # a.copy(b, "bitcoin", "bitcoinkeywords")