import logging
import os
import getpass
import datetime as dt
import sys
import functools
import traceback


def get_logger(logger_, application: str):
    create_logging_dir()

    fh = logging.FileHandler(f'/home/{getpass.getuser()}/log/{application}_'
                             f'{dt.datetime.now().strftime("%Y-%m-%d_%H%m")}.log')
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('(%(asctime)s) (%(funcName)-8s) [%(levelname)-5s] [%(processName)-8s]: %(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    logger = logging.getLogger(logger_)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(sh)
    logger.addHandler(fh)

    logger.info(f'*{"*" * (len(application)+2)}*')
    logger.info(f'* {application} *')
    logger.info(f'*{"*" * (len(application)+2)}*')
    logger.info("Started at:")
    logger.info(f'{dt.datetime.now()}')
    logger.info("Executable path:")
    logger.info(f"{sys.executable}")
    logger.info("Current system path:")
    logger.info(f"{sys.path}")
    logger.info("Platform:")
    logger.info(f"{sys.platform}")
    logger.info("Version:")
    logger.info(f"{sys.version}")
    logger.info("Passed args:")
    logger.info(f"{sys.argv}")
    logger.info("Current working directory")
    logger.info(f"{os.getcwd()}")

    return logger


def create_logging_dir():
    log_dir = f'/home/{getpass.getuser()}/log'
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)


def log_on_failure(function):
    """
    Decorator that wraps the passed in function and logs the exception should one occur
    """
    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            output = function(*args, **kwargs)
            print(output)
        except Exception as e:
            logging.error(e)
            error = traceback.format_exc()
            logging.exception(error)
    return wrapper