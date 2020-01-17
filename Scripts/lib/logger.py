import logging
import os
import getpass
import datetime as dt
import sys
import functools
import traceback

import lib.alerting
from lib.config_parser import ConfigNode


def get_logger(application: str):
    create_logging_dir()

    fh = logging.FileHandler(f'/home/{getpass.getuser()}/log/{application}_'
                             f'{dt.datetime.now().strftime("%Y-%m-%d_%H%m")}.log')
    sh = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s ((funcName)-5s) [%(levelname)-5s] [%(processName)-8s]: %(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    fh.setLevel(logging.INFO)
    sh.setLevel(logging.INFO)

    logger = logging.getLogger()
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


def log_config(config_node: ConfigNode) -> None:
    def log_dict(config_dict: dict, recurse_num):
        for key, value in config_dict.items():
            if isinstance(value, dict):
                logging.info(f"{' ' * recurse_num * 2}[{key}]")
                log_dict(value, recurse_num + 1)
            else:
                logging.info(f"{' ' * (recurse_num * 3)}{key} = {value}")
    for k, v in config_node.items():
        if isinstance(v, dict):
            logging.info(f"[{k}]")
            log_dict(v, 1)
        else:
            logging.info(f"{' ' * 2}{k} = {v}")


def app_main(logger, alerter: lib.alerting.Alert = None):
    """
    TODO should this be in a different module?
    Decorator to replace log on failure, wrap the main and this will log and alert, this is the function that will call
    the final alerting as well
    """
    def wrap(f):
        def wrapper(*args, **kwargs):
            try:
                output = f(*args, **kwargs)
                logger.info(output)
                return_code = 0
            except Exception as e:
                logger.error(e)
                error = traceback.format_exc()
                logger.exception(error)
                if alerter is not None:
                    alerter.error("Error in script")
                    alerter.error(error)
                return_code = 1
            if alerter is not None:
                alerter.send_message(app)
            return return_code
        return wrapper
    return wrap
