import pandas as pd
from argparse import ArgumentParser
import lib.logger as logger
from lib.database import Database, DbInfo
from lib.alerting import get_alerter

__app__ = 'load_sec_universe'
logger = logger.get_logger(__name__, __app__)
alerter = get_alerter()

DB_MAP = {'Symbol_x': 'SYMBOLNYSE', 'CQS_Symbol': 'CQSSYMBOL', 'SymbolIndexNumber': 'SYMBOLINDEXNUMBER',
          'Symbol_y': 'NASDAQSYMBOL'}

UP_INS_STMTS = {'UPDATE': "UPDATE {schema}.{table} SET SYMBOLNYSE='{nyse}', SYMBOLINDEXNUMBER='{nyseind}', "
                          "NASDAQSYMBOL='{nasd}' WHERE CQSSYMBOL='{cqs};",
                'INSERT': "INSERT INTO {schema}.{table} VALUES ({symid}, '{nyse}', {nyseind}, '{nasd}', '{cqs}');"}


def get_symbols(db_info: DbInfo) -> pd.DataFrame:
    query = f"SELECT SYMBOLNYSE, NASDAQSYMBOL, CQSSYMBOL FROM {db_info.schema}.{db_info.table}"
    logger.info(query)
    df = db_info.database.get_df(query)
    logger.info(f"Database df shape: {df.shape}")
    if not len(df):
        df = pd.DataFrame(columns=['SYMBOLNYSE', 'NASDAQSYMBOL', 'CQSSYMBOL'])

    return df


def get_csv_dfs(path: str, sep) -> pd.DataFrame:
    arca_df = pd.read_csv(f'{path}/symbols_arca.csv', sep='|', index_col=0)
    nyse_df = pd.read_csv(f'{path}/symbols_nyse.csv', sep='|', index_col=0)

    # convert to int cause of stupid mysql
    nyse_df['SymbolIndexNumber'] = nyse_df['SymbolIndexNumber'].astype(int)
    arca_df['SymbolIndexNumber'] = arca_df['SymbolIndexNumber'].astype(int)
    # nas_lis = pd.read_csv(f'{path}/symbols_nasdaq_listed.csv')  # since this one will make it 10x harder fuck it
    nas_trd = pd.read_csv(f'{path}/symbols_nasdaq_traded.csv', sep='|', index_col=0)
    logger.info(f"Arca df size: {arca_df.shape}")
    logger.info(f"nyse df size: {nyse_df.shape}")
    logger.info(f"Nasdaq traded df size: {nas_trd.shape}")

    nyse_cols = ['Symbol', 'CQS_Symbol', 'SymbolIndexNumber']
    nasdaq_cols = ['Symbol', 'CQS_Symbol']
    nyse_vals = pd.merge(arca_df, nyse_df, 'outer', on=nyse_cols)[nyse_cols]
    nasdaq_add = pd.merge(nyse_vals, nas_trd[nasdaq_cols], 'outer', on=['CQS_Symbol'])

    logger.info(f"Final frame shape: {nasdaq_add.shape}")
    alerter.info(f"Final frame shape: {nasdaq_add.shape}")

    return nasdaq_add.rename(columns=DB_MAP)


def max_value(db_info):
    query = f"SELECT MAX(SYMBOLID) AS MAX_SYM FROM {db_info.schema}.{db_info.table}"
    logger.info(query)
    max_val = db_info.database.execute(query)[0]['MAX_SYM']
    if max_val is not None:
        return max_val
    else:
        return 0


def nullify_str(input_str: str) -> str:
    output_str = input_str.replace("'nan'", "NULL")
    output_str = output_str.replace("nan", "NULL")
    return output_str


def gen_sql_stmts(sym_df: pd.DataFrame, curr_df: pd.DataFrame, max_val: int, schema: str, table: str) -> tuple:
    old_cqs_syms = sym_df['CQSSYMBOL'].to_list()
    updates = []
    inserts = []

    for index, row in curr_df.iterrows():
        if row['CQSSYMBOL'] in old_cqs_syms:
            update_stmt = nullify_str(UP_INS_STMTS['UPDATE'].format(schema=schema, table=table, nyse=row['SYMBOLNYSE'],
                                                        nyseind=row['SYMBOLINDEXNUMBER'], nasd=row['NASDAQSYMBOL'],
                                                        cqs=row['CQSSYMBOL']))
            logger.info(update_stmt)
            updates.append(update_stmt)
        else:
            insert_stmt = nullify_str(UP_INS_STMTS['INSERT'].format(schema=schema, table=table, symid=max_val,
                                                        nyse=row['SYMBOLNYSE'], nyseind=row['SYMBOLINDEXNUMBER'],
                                                        nasd=row['NASDAQSYMBOL'], cqs=row['CQSSYMBOL']))
            logger.info(insert_stmt)
            inserts.append(insert_stmt)
            max_val += 1

    logger.info(f"Update statements: {len(updates)}, Insert statements: {len(inserts)}")

    return updates, inserts


def run_stmts(update, insert, db_info):
    try:
        logger.info("Updating database")
        db_info.database.execute('\n '.join(update))
        logger.info("Successfully updated database")
        alerter.info("Successfully updated database")
    except Exception as e:
        logger.error(e)
        logger.error("Failed to update db")
        alerter.error("Failed to update db")

    try:
        logger.info("Inserting into database")
        for ins in insert:
            logger.info(ins)
            db_info.database.execute(ins)
        logger.info("Successfully inserted into database")
        alerter.info("Successfully inserted into database")
    except Exception as e:
        logger.error(e)
        logger.error("Failed to insert into db")
        alerter.error("Failed to insert into db")


def main_impl(args):
    db_info = DbInfo(Database(args.database), args.schema, args.table)

    sym_df = get_symbols(db_info)
    curr_df = get_csv_dfs(args.path, args.sep)

    max_val = max_value(db_info)

    updates, inserts = gen_sql_stmts(sym_df, curr_df, max_val, db_info.schema, db_info.table)

    run_stmts(updates, inserts, db_info)


def main():
    parser = ArgumentParser(description="This script will read in the files and "
                                        "append them 'correctly' to the database")
    parser.add_argument('--database', help='database account to use', default='DATABASE_INSERTER')
    parser.add_argument('--schema', help='Schema where the securities are', default='FREEOHLC')
    parser.add_argument('--table', help='name of the table', default='SECURITY')
    parser.add_argument('--path', help='path to all the csv files', default='/tmp')
    parser.add_argument('--sep', help='csv separator', default='|')
    args = parser.parse_args()

    try:
        main_impl(args)
    except Exception as e:
        logger.info(e)
        alerter.error("Something happened check log files")
        alerter.error(e)

    alerter.send_message(__app__)


if __name__ == '__main__':
    main()
