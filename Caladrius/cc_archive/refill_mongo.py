import logging
import traceback
from os.path import abspath, dirname, join

import pandas as pd

from caladrius.auxiliary import logging_util
from cc_archive.cc_db import get_cc_order

__here__ = abspath(dirname(__file__))
log_filepath = join(__here__, 'logs', f'refill_mongo.log')
logger = logging_util.get_csv_logger(log_filepath, file_level=logging.INFO)
logger.debug('hello')

already_got = pd.read_csv(log_filepath, names=logging_util.get_csv_headers(False))
already_got = already_got['Message'].str.extract(r'^Got \[(0[0234]-\d+)]$')[0]

df = pd.read_csv('results_not_in_mongodb.csv')
mrns_not_in_mongo = set(df['mrn'])

for mrn in mrns_not_in_mongo - set(already_got):
    try:
        get_cc_order(mrn)
        logger.info(f'Got [{mrn}]')
    except:
        logger.warning(f'Exception with [{mrn}]')
        traceback.print_exc()
    # sleep(1)
    # break
