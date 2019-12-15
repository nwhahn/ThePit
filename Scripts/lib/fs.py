import datetime as dt
import os
import logging
import glob

import pandas as pd


def get_version(path: str, file_name: str) -> int:
    files = glob.glob(f'{path}/{file_name}.*')
    logging.info(files)
    if len(files) == 0:
        return 0
    else:
        return max([int(v.split('.')[-1]) for v in files]) + 1


def make_symlink(df: pd.DataFrame, path: str, sym_name: str, sep: str) -> None:
    today = dt.datetime.today().strftime('%Y_%m_%d')
    creation_file = f'{sym_name}_{today}.csv'
    logging.info(f'Generating file with name: {creation_file}')
    version = get_version(path, creation_file)

    true_path = f'{path}/{creation_file}.{version}'
    logging.info(f'Writing out csv to {true_path}')
    df.to_csv(true_path, sep=sep)

    symlink_path = f'{path}/{sym_name}.csv'
    if os.path.exists(symlink_path):
        logging.info(f"Unlinking {symlink_path}")
        os.unlink(symlink_path)
    os.symlink(true_path, symlink_path)
