import datetime as dt
from argparse import ArgumentParser

import pandas as pd

from lib.logger import get_logger, app_main
from lib.database import Database, DbInfo
from lib.alerting import get_alerter
import lib.config_parser as config_parser

__app__ = 'load_sec_universe'
logger = get_logger(__app__)
alerter = get_alerter(__app__)

DB_MAP = {'Symbol_x': 'SYMBOLNYSE', 'CQS_Symbol': 'CQSSYMBOL', 'SymbolIndexNumber': 'SYMBOLINDEXNUMBER',
          'Symbol_y': 'NASDAQSYMBOL'}

UP_INS_STMTS = {'UPDATE': "UPDATE {schema}.{table} SET SYMBOLNYSE='{nyse}', SYMBOLINDEXNUMBER={nyseind}, "
                          "NASDAQSYMBOL='{nasd}' WHERE CQSSYMBOL='{cqs}';",
                'INSERT': "INSERT INTO {schema}.{table} VALUES ({symid}, '{nyse}', '{cqs}', {nyseind}, '{nasd}', "
                          "'{today}');"}


def get_symbols(db_info: DbInfo) -> pd.DataFrame:
    query = f"SELECT SYMBOLNYSE, NASDAQSYMBOL, CQSSYMBOL FROM {db_info.schema}.{db_info.table}"
    logger.info(query)
    df = db_info.database.get_df(query)
    logger.info(f"Database df shape: {df.shape}")
    if not len(df):
        df = pd.DataFrame(columns=['SYMBOLNYSE', 'NASDAQSYMBOL', 'CQSSYMBOL'])

    return df


def get_csv_dfs(path: str, sep: str) -> pd.DataFrame:
    arca_df = pd.read_csv(f'{path}/arca.csv', sep=sep, index_col=0)
    nyse_df = pd.read_csv(f'{path}/nyse.csv', sep=sep, index_col=0)

    # convert to int cause of stupid mysql
    nyse_df['SymbolIndexNumber'] = nyse_df['SymbolIndexNumber'].astype(int)
    arca_df['SymbolIndexNumber'] = arca_df['SymbolIndexNumber'].astype(int)
    # nas_lis = pd.read_csv(f'{path}/symbols_nasdaq_listed.csv')  # since this one will make it 10x harder fuck it
    nas_trd = pd.read_csv(f'{path}/nasdaq_traded.csv', sep=sep, index_col=0)
    logger.info(f"Arca df size: {arca_df.shape}")
    logger.info(f"nyse df size: {nyse_df.shape}")
    logger.info(f"Nasdaq traded df size: {nas_trd.shape}")

    nyse_cols = ['Symbol', 'CQS_Symbol', 'SymbolIndexNumber']
    nasdaq_cols = ['Symbol', 'CQS_Symbol']

    nyse_vals = pd.merge(arca_df, nyse_df, 'outer', on=nyse_cols)[nyse_cols]
    nyse_vals['SymbolIndexNumber'] = nyse_vals['SymbolIndexNumber'].astype(int)

    nasdaq_add = pd.merge(nyse_vals, nas_trd[nasdaq_cols], 'outer', on=['CQS_Symbol'])

    logger.info(f"Final frame shape: {nasdaq_add.shape}")
    alerter.info(f"Total symbols today: {len(nasdaq_add)}")

    return nasdaq_add.rename(columns=DB_MAP)


def max_value(db_info):
    query = f"SELECT MAX(SYMBOLID) AS MAX_SYM FROM {db_info.schema}.{db_info.table}"
    logger.info(query)
    max_val = db_info.database.execute(query)[0]['max_sym']
    if max_val is not None:
        return max_val
    else:
        return 0


def nullify_str(input_str: str) -> str:
    output_str = input_str.replace("'nan'", "NULL")
    output_str = output_str.replace("nan", "NULL")
    return output_str


def intify(input_):
    """this function benefits from no type hints"""
    if pd.isna(input_):
        return input_
    else:
        return int(input_)


def gen_sql_stmts(sym_df: pd.DataFrame, curr_df: pd.DataFrame, max_val: int, schema: str, table: str) -> tuple:
    if len(sym_df) > 0:
        old_cqs_syms = sym_df['cqssymbol'].tolist()
    else:
        old_cqs_syms = []
    updates = []
    inserts = []

    for index, row in curr_df.iterrows():
        if row['CQSSYMBOL'] in old_cqs_syms:
            update_stmt = nullify_str(UP_INS_STMTS['UPDATE'].format(schema=schema, table=table, nyse=row['SYMBOLNYSE'],
                                                                    nyseind=intify(row['SYMBOLINDEXNUMBER']),
                                                                    nasd=row['NASDAQSYMBOL'], cqs=row['CQSSYMBOL']))
            logger.info(update_stmt)
            updates.append(update_stmt)
        else:
            print(type(row['SYMBOLINDEXNUMBER']))
            insert_stmt = nullify_str(UP_INS_STMTS['INSERT'].format(schema=schema, table=table, symid=max_val,
                                                                    nyse=row['SYMBOLNYSE'],
                                                                    nyseind=intify(row['SYMBOLINDEXNUMBER']),
                                                                    nasd=row['NASDAQSYMBOL'], cqs=row['CQSSYMBOL'],
                                                                    today=dt.date.today()))
            logger.info(insert_stmt)
            inserts.append(insert_stmt)
            max_val += 1

    logger.info(f"Update statements: {len(updates)}, Insert statements: {len(inserts)}")

    return updates, inserts


def main_impl(config: config_parser.ConfigNode):
    db_info = DbInfo(Database(config, config['sec_univ.db_acc']), config['sec_univ.schema'], config['sec_univ.table'])

    sym_df = get_symbols(db_info)
    curr_df = get_csv_dfs(config['sec_univ.path'], config['sec_univ.sep'])

    max_val = max_value(db_info)

    updates, inserts = gen_sql_stmts(sym_df, curr_df, max_val, db_info.schema, db_info.table)

    updates_len = len(updates)
    inserts_len = len(inserts)

    logger.info(updates[:2])

    logger.info("Updating database")
    if len(updates) > 0:
        db_info.database.execute('\n '.join(updates))
    logger.info(f"Successfully updated {updates_len} symbols in database")
    alerter.info(f"Successfully updated {updates_len} symbols in database")

    logger.info("Inserting into database")
    for ins in inserts:
        logger.info(ins)
        db_info.database.execute(ins)
    logger.info(f"Successfully inserted {inserts_len} symbols into database")
    alerter.info(f"Successfully inserted {inserts_len} symbols into database")


@app_main(logger, alerter)
def main():
    parser = ArgumentParser(description="This script will read in the files and "
                                        "append them 'correctly' to the database")
    config = config_parser.config_argparse(parser)

    main_impl(config)


if __name__ == '__main__':
    main()
