import logging

from conf_common import default_spark_log_dir


def initialize_default_spark_logger():

    set_logging_config(filename=default_spark_log_dir,
                       level='DEBUG',
                       format='%(asctime)s %(message)s')


def get_loggers(name):
    logger = logging.getLogger(name)

    # if len(logger.handlers) == 0:
    #     initialize_default_spark_logger()

    def print(msg):
        logger.debug(msg)

    def log_info(msg):
        logger.info(msg)

    def log_error(msg):
        logger.error(msg)

    return print, log_info, log_error

from datetime import datetime

def set_logging_config(*args, **kwargs):
    logging.basicConfig(*args, **kwargs)


def set_logging_level(level, logger_name = None):
    if logger_name:
        return logging.getLogger(logger_name).setLevel(level)
    else:
        return logging.getLogger(logger_name).setLevel(level)


def get_new_logfile_name():
    return "{}.log".format(datetime.now().strftime('%Y_%m_%d-%H_%M'))