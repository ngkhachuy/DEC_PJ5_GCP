import datetime
import logging

MONGODB_LOCALHOST = "mongodb://localhost:27017/"
MONGODB_DB_NAME = "TIKI_NEW"


def print_execution_time(LOGGER, started):

    finished = datetime.datetime.now()
    LOGGER.info('STARTED TIME  : ' + started.strftime("%H:%M:%S %d/%m/%Y"))
    LOGGER.info('FINISHED TIME : ' + finished.strftime("%H:%M:%S %d/%m/%Y"))
    LOGGER.info('EXECUTION TINE: ' + str(finished - started))


def get_log(file_log):
    logging.basicConfig(filename=file_log,
                        filemode='a',
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%d-%m-%Y %H:%M:%S',
                        level=logging.INFO)
    return logging.getLogger()
