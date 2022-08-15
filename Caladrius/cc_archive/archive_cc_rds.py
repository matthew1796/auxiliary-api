import logging
import re
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from os.path import abspath, dirname, join
from threading import Thread

import pandas as pd
import time
from sqlalchemy.orm import sessionmaker

from caladrius.Covid_Clinic import LoginUtil
from caladrius.DocumentDB import MongoClient
from caladrius.RabbitMQ import RabbitMQChannel
from caladrius.SQL import SQLAgent, merge_if_dne_on_pkeys
from caladrius.auxiliary import logging_util
from cc_archive import serialize_patient, serialize_billing, Symptom, LineItem
from cc_archive.cc_db import get_cc_order

pd.options.display.width = 0

__here__ = abspath(dirname(__file__))

archive_cc_rds_channel_name = 'archive-cc-rds'

consumer_log_filepath = join(__here__, 'logs', 'consume_archive_cc_rds.log')
# trfh = TimedRotatingFileHandler(consumer_log_filepath, when='midnight', utc=True, encoding='utf-8')
consumer_logger = logging_util.get_csv_logger(consumer_log_filepath, file_level=logging.INFO, multithread=True)
n_workers = 3

archive_threshold = 250


def consume_archive_cc_rds():
    class ArchiveDriver(Thread):
        def __init__(self):
            Thread.__init__(self)
            self.loginutil = LoginUtil()
            self.token = self.loginutil.get_token(60 * 12)

        def run(self) -> None:
            sql_agent = SQLAgent('production')
            session = sessionmaker(bind=sql_agent.get_engine('covid_clinic', pool_pre_ping=True))()

            with RabbitMQChannel() as channel:
                channel.queue_declare(queue=archive_cc_rds_channel_name, durable=True)
                with MongoClient('master') as db_client:
                    mrns = []
                    delivery_tags = []
                    while True:
                        method, _, msg_body = channel.basic_get(archive_cc_rds_channel_name, auto_ack=True)

                        if msg_body is not None:
                            mrns.append(msg_body.decode())
                            delivery_tags.append(method.delivery_tag)
                            print(f'Appending {msg_body.decode()} to {self.name} ({len(mrns)}/{archive_threshold})')

                        if len(mrns) >= archive_threshold or msg_body is None:
                            retrieved_orders = get_cc_order(mrns, this_db_client=db_client,
                                                            amend_collection_timestamp=False)
                            for (mrn, order), delivery_tag in zip(retrieved_orders.items(), delivery_tags):
                                if not order:
                                    consumer_logger.error(f'No order returned for mrn=[{mrn}]')
                                    # channel.basic_ack(delivery_tag=delivery_tag)
                                    continue

                                try:
                                    if mrn != order.patient.mrn:
                                        raise ValueError(
                                            f'MRN mismatch mrn=[{mrn}] != patient.mrn=[{order.patient.mrn}]')

                                    my_patient = serialize_patient(order.patient)
                                    my_billing = serialize_billing(order.patient)
                                    merged = merge_if_dne_on_pkeys(session, my_patient)
                                    merged |= merge_if_dne_on_pkeys(session, my_billing)
                                    if order.patient.symptoms.symptoms is not None:
                                        for symptom_name in order.patient.symptoms.symptoms:
                                            merged |= merge_if_dne_on_pkeys(session, Symptom(mrn=mrn,
                                                                                             symptom=symptom_name.strip(),
                                                                                             creation_timestamp=datetime.utcnow()))
                                    for line_item_name in order.line_item_names:
                                        merged |= merge_if_dne_on_pkeys(session, LineItem(mrn=mrn,
                                                                                          line_item_name=line_item_name,
                                                                                          creation_timestamp=datetime.utcnow()))
                                    if merged:
                                        consumer_logger.info(f'Archived [{mrn}]')
                                    else:
                                        consumer_logger.debug(f'Already archived [{mrn}]')
                                    # consumer_logger.debug(f'ACKing {mrn} <{delivery_tag}>')
                                    # channel.basic_ack(delivery_tag=delivery_tag)

                                except Exception as e:
                                    session.rollback()
                                    # traceback.print_exc()
                                    consumer_logger.exception(e)
                                    if msg_body is not None:
                                        consumer_logger.exception(e)
                                        consumer_logger.error(f'Exception {mrn} <{delivery_tag}>')
                                    time.sleep(5)
                            session.commit()
                            mrns = []
                            delivery_tags = []

                        if msg_body is None:
                            channel.connection.process_data_events()
                            session = sessionmaker(bind=sql_agent.get_engine('covid_clinic', pool_pre_ping=True))()
                            sleep_time = 60
                            consumer_logger.debug(f'Nothing in the queue, sleeping for {sleep_time} seconds')
                            time.sleep(sleep_time)

    for i in range(n_workers):
        worker = ArchiveDriver()
        worker.daemon = False
        time.sleep(10)
        worker.name = f'Archiver_{i}'
        worker.start()


publisher_log_filepath = join(__here__, 'logs', 'publish_archive_cc_rds.log')
publisher_logger = logging_util.get_csv_logger(publisher_log_filepath, file_level=logging.INFO, multithread=True)


def publish_archive_cc_rds(mrns: str | list):
    if isinstance(mrns, str):
        mrns = [mrns]

    with RabbitMQChannel() as channel:
        channel.queue_declare(queue=archive_cc_rds_channel_name, durable=True)
        counter = 0
        for mrn in mrns:
            counter += 1
            if re.match(r'0[0234]-\d+', mrn):
                publisher_logger.debug(f'Queuing [{mrn}]')
                channel.basic_publish(exchange='',
                                      routing_key=archive_cc_rds_channel_name,
                                      body=mrn.encode())
                publisher_logger.info(f'Queued [{mrn}]')
            if counter % 50 == 0:
                time.sleep(0.05)


if __name__ == '__main__':
    consumer_logger.info(f'Restarting archive_cc_rds.py with {n_workers} workers '
                         f'and an archive threshold of {archive_threshold}')
    consume_archive_cc_rds()
