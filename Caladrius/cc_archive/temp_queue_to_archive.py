import hashlib
import json
import logging
import traceback
from datetime import datetime
from os.path import abspath, dirname, join
from threading import Thread

import pandas as pd
import requests
import time

from caladrius.Covid_Clinic import source_woocommerce, source_ccadmin, wc_api, LoginUtil
from caladrius.DocumentDB import MongoClient
from caladrius.RabbitMQ import RabbitMQChannel
from caladrius.auxiliary import logging_util

if __name__ == '__main__':

    __here__ = abspath(dirname(__file__))
    log_filepath = join(__here__, 'logs', 'temp_queue_to_archive.log')
    logger = logging_util.get_csv_logger(log_filepath, file_level=logging.INFO, multithread=True)

    # already_stashed = pd.read_csv(log_filepath, names=logging_util.get_csv_headers(multithread=True))
    # already_stashed = already_stashed['Message'].str.extract(r'^Stashed \[(0[0234]-\d+)]$')[0]
    stash_threshold = 10
    temp_channel_name = 'temp1'
    bad_channel_name = 'bad2'


    class StashDriver(Thread):
        def __init__(self):
            Thread.__init__(self)
            self.loginutil = LoginUtil()
            self.token = self.loginutil.get_token(60 * 12)

        def run(self) -> None:
            with requests.Session() as sesh:
                sesh.headers.update({'Authorization': 'Bearer {}'.format(self.token)})
                with RabbitMQChannel() as channel:
                    channel.queue_declare(queue=temp_channel_name, durable=True)
                    channel.queue_declare(queue=bad_channel_name, durable=True)

                    with MongoClient('master') as db_client:
                        orders_collection = db_client['covidclinic']['notes']
                        mrns = []
                        delivery_tags = []
                        while True:
                            method, _, msg_body = channel.basic_get(temp_channel_name, auto_ack=False)

                            # logger.debug(f'Putting on {msg_body.decode()} <{method.delivery_tag}>')

                            if msg_body is not None:
                                mrns.append(msg_body.decode())
                                delivery_tags.append(method.delivery_tag)
                                print(f'Appending {msg_body.decode()} to {self.name} ({len(mrns)}/{stash_threshold})')

                            if len(mrns) >= stash_threshold or msg_body is None:
                                document_bodies = []
                                for mrn, delivery_tag in zip(mrns, delivery_tags):
                                    logger.debug(f'Stashing [{mrn}]')
                                    try:
                                        site_code = mrn[0:2]
                                        order_number = mrn[3:]
                                        stage = 'production'

                                        if site_code == '00':
                                            source_of_truth = source_woocommerce
                                        elif site_code == '02':
                                            if int(order_number) >= 1000000:
                                                source_of_truth = source_woocommerce
                                            else:
                                                source_of_truth = source_ccadmin
                                        elif site_code == '03' or site_code == '04':
                                            source_of_truth = source_ccadmin

                                        order_response = None
                                        if source_of_truth == source_woocommerce:
                                            order_response = wc_api[stage][site_code].get(
                                                f"orders/{order_number}")
                                            if order_response.status_code == 404 and site_code == '00':
                                                stage = 'archive'
                                                order_response = wc_api[stage][site_code].get(
                                                    f"orders/{order_number}")
                                            order_response.close()

                                        if not order_response:
                                            source_of_truth = source_ccadmin
                                            stage = 'production'

                                        if source_of_truth == source_ccadmin:
                                            order_response = sesh.get(
                                                'https://api.covidclinic.org/api/v1/storefront',
                                                params={'mrn': mrn})
                                            if not order_response:
                                                self.token = self.loginutil.get_token(60 * 12)
                                                sesh.headers.update({'Authorization': 'Bearer {}'.format(self.token)})
                                                order_response = sesh.get(
                                                    'https://api.covidclinic.org/api/v1/storefront',
                                                    params={'mrn': mrn})
                                            order_response.close()

                                        if order_response is None:
                                            raise ValueError(f'Order response for [{mrn}] is None')
                                        payload_body = order_response.json()
                                        if payload_body is None:
                                            raise ValueError(f'Payload body for [{mrn}] is None')
                                        if (source_of_truth == source_ccadmin) and not payload_body.get('total'):
                                            raise ValueError(f'CCAdmin returned no results for [{mrn}]')

                                        data_md5 = hashlib.md5(
                                            json.dumps(payload_body, sort_keys=True).encode()).hexdigest()
                                        document_bodies.append({
                                            'mrn': mrn,
                                            'source_of_truth': source_of_truth,
                                            'stage': stage,
                                            'raw_body': payload_body,
                                            'md5(raw_body)': data_md5,
                                            'date_created': datetime.utcnow().isoformat()
                                        })
                                        logger.info(f'Stashed [{mrn}]')

                                        logger.debug(f'ACKing {mrn} <{delivery_tag}>')
                                        channel.basic_ack(delivery_tag=delivery_tag)

                                    except Exception as e:
                                        traceback.print_exc()
                                        logger.exception(e)
                                        if msg_body is not None:
                                            # logger.debug(f'!NACKing {mrn} <{delivery_tag}>')
                                            # channel.basic_nack(delivery_tag=delivery_tag)
                                            logger.debug(f'Resource not found {mrn} <{delivery_tag}>')
                                            channel.basic_ack(delivery_tag=delivery_tag)
                                            channel.basic_publish(exchange='',
                                                                  routing_key=bad_channel_name,
                                                                  body=msg_body)
                                        time.sleep(5)

                                if document_bodies:
                                    print(f'Inserting {stash_threshold} document(s) '
                                          f'[{", ".join([x["mrn"] for x in document_bodies])}]')
                                    logger.info(
                                        f'Inserting {stash_threshold} document(s) '
                                        f'[{", ".join([x["mrn"] for x in document_bodies])}]')
                                    orders_collection.insert_many(document_bodies)
                                mrns = []
                                delivery_tags = []

                            if msg_body is None:
                                break


    n_workers = 10

    for i in range(n_workers):
        worker = StashDriver()
        worker.daemon = False
        worker.name = f'Stasher_{i}'
        worker.start()
    print()
