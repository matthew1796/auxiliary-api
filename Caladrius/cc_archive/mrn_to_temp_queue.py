import logging
import re
import time
from os.path import abspath, dirname, join
import pandas as pd
from caladrius.RabbitMQ import RabbitMQChannel
from caladrius.auxiliary import logging_util

if __name__ == '__main__':
    from primary_automation.billing.audit import result_tables
    #
    results_df = result_tables.df

    __here__ = abspath(dirname(__file__))
    temp_channel_name = 'temp1'
    log_filepath = join(__here__, 'logs', f'mrn_to_{temp_channel_name}_queue.log')
    logger = logging_util.get_csv_logger(log_filepath, file_level=logging.INFO)

    rabbit_mq_channel = RabbitMQChannel()

    already_queued = pd.read_csv(log_filepath, names=logging_util.get_csv_headers(False))
    # already_queued = already_queued['Message'].str.extract(r'^Queued \[(0[0234]-\d+)]$')[0]
    already_queued = already_queued['Message'].str.extract(r'^Queued \[(0[02]-\d+)]$')[0]
    # results_not_in_mongo = pd.read_csv('results_not_in_mongodb.csv')
    # missing_mrns = results_not_in_mongo['mrn']

    with rabbit_mq_channel as channel:
        channel.queue_declare(queue=temp_channel_name, durable=True)
        counter = 0
        results_df.drop_duplicates(subset=['mrn'], keep='first', inplace=True)
        for i, result_entry in results_df[~results_df['mrn'].isin(already_queued)].iterrows():
        # for i, result_entry in results_not_in_mongo.iterrows():
            counter += 1
            mrn = result_entry['mrn']
            # if re.match(r'0[0234]-\d+', mrn):
            if re.match(r'0[02]-\d+', mrn):
                logger.debug(f'Queuing [{mrn}]')
                channel.basic_publish(exchange='',
                                      routing_key=temp_channel_name,
                                      body=mrn.encode())
                logger.info(f'Queued [{mrn}]')
            if counter % 10 == 0:
                time.sleep(0.2)
