import logging
import functools
import traceback

logger = logging.getLogger('google_trends_analysis')


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
            logger.error(e)
            error = traceback.format_exc()
            logger.exception(error)
    return wrapper
