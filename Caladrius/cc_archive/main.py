import json
import logging
import re
import traceback
from copy import copy
from glob import glob
from os.path import abspath, dirname, join, getctime
from queue import Queue
from random import random
from threading import Thread, Lock

import dateutil.parser
import pandas as pd
import time
from requests import HTTPError, ReadTimeout, RequestException

from caladrius.Covid_Clinic import CC_MRN, source_woocommerce, source_ccadmin, get_order, WooCommerce, ccadmin
from caladrius.RabbitMQ import RabbitMQChannel
from caladrius.auxiliary import logging_util
from caladrius.core.entities import BadPHIException
from primary_automation import OStatus, parse_ostatus
from primary_automation.billing import BillingDriver
from resources.client_data.facilities import client_facility_map
from resources.client_data.providers import client_provider_map


# class StashDriver(Thread):
#     def __init__(self, task_queue: Queue, processed_queue: Queue, billing_driver: BillingDriver):
#         Thread.__init__(self)
#         self.task_queue = task_queue
#         self.processed_queue = processed_queue
#         self.billing_driver = billing_driver
#         self.idx_cache = []
#         self.ostatus_cache = []
#
#     def run(self) -> None:
#         while True:
#             idx, mrn, result_timestamp = self.task_queue.get()
#             ostatus = OStatus.PROCESSING
#
#             try:
#                 cc_mrn = CC_MRN(mrn)
#                 api_connection_attempt = 0
#                 successful_api_connection = False
#                 order = None
#                 while api_connection_attempt < max_api_connection_attempt and not successful_api_connection:
#                     api_connection_attempt += 1
#                     try:
#                         source_of_truth = None
#
#                         if cc_mrn.site_code == '00' or cc_mrn.site_code == '02':
#                             source_of_truth = source_woocommerce
#
#                         # if (cc_mrn.site_code == '02') and (int(cc_mrn.order_id) < 1000000):  # per Thomas
#                         #     source_of_truth = source_ccadmin
#
#                         if cc_mrn.site_code == '03' or cc_mrn.site_code == '04':
#                             source_of_truth = source_ccadmin
#
#                         if source_of_truth == source_woocommerce:
#                             order_data = get_order(mrn, source=source_of_truth)
#
#                             # Check archive...
#                             # if (order_data is None or order_data.status_code == 404) and (cc_mrn.site_code == '00'):
#                             #     order_data = get_order(mrn, stage='archive', source=source_woocommerce)
#                             #     ostatus |= OStatus.RESOURCE_NOT_FOUND
#                             # else:
#                             #     ostatus &= ~OStatus.RESOURCE_NOT_FOUND
#
#                             # Per Alan Fox and Brian Hawkins on 2022-05-13, check ccadmin if not in woocommerce
#                             if order_data is None or order_data.status_code == 404:
#                                 # source_of_truth = source_ccadmin
#                                 ostatus |= OStatus.RESOURCE_NOT_FOUND
#                             else:
#                                 order_data.raise_for_status()
#                                 successful_api_connection = True
#                                 ostatus &= ~OStatus.RESOURCE_NOT_FOUND
#                                 order_json = order_data.json()
#                                 if order_json.get('patient_information') is None:
#                                     raise BadPHIException()
#                                 order = WooCommerce.parse_order(order_json=order_data.json())
#                                 # Fix collection timestamp issue
#                                 if not order.specimen.collection_datetime:
#                                     collection_datetime = None
#                                     try:
#                                         collection_datetime = WooCommerce.get_latest_label_print_time(mrn)
#                                     except Exception as e:
#                                         pass
#                                     order.specimen.collection_datetime = collection_datetime
#
#                         if source_of_truth == source_ccadmin:
#                             order_data = get_order(mrn, source=source_ccadmin)
#
#                             order_data.raise_for_status()
#                             successful_api_connection = True
#
#                             order_data = order_data.json()
#                             if order_data['results'] is None:
#                                 ostatus |= OStatus.RESOURCE_NOT_FOUND
#                             else:
#                                 ostatus &= ~OStatus.RESOURCE_NOT_FOUND
#
#                             n_results = len(order_data['results'])
#                             if n_results != 1:
#                                 ostatus |= OStatus.RESOURCE_NOT_FOUND
#                                 raise ValueError(
#                                     f'Unexpected number of results returned for {mrn}: {n_results} (expected 1)')
#                             order_data = order_data['results'][0]
#                             order = ccadmin.parse_order(order_data)
#                     except (ConnectionError, HTTPError, ReadTimeout) as err:
#                         successful_api_connection = False
#                         if api_connection_attempt >= max_api_connection_attempt:
#                             error_msg = f'{type(err).__name__}: Error {err.response.status_code}: {err.response.reason}. ' \
#                                         f'Max API connection attempts reached'
#                             logger.error(error_msg)
#                             ostatus |= OStatus.FAIL_CONNECT_CLIENT
#
#                         else:
#                             logger.error(
#                                 f'{type(err).__name__}: Error {err.response.status_code}: {err.response.reason} '
#                                 f'(Attempt {api_connection_attempt}/{max_api_connection_attempt}). '
#                                 f'Sleeping for 1s...')
#                             traceback.print_exc()
#                             time.sleep(1)
#                             continue
#                     except RequestException as req_exception:
#                         traceback.print_exc()
#                         logger.error(f'Request exception: {str(req_exception)}')
#                         ostatus |= OStatus.FAIL_CONNECT_CLIENT
#
#                 if order is None:
#                     logger.warning(f'Unable to process mrn=[{mrn}]')
#                 elif not order.specimen.collection_datetime:
#                     ostatus |= OStatus.FAILED_TO_PARSE_SPECIMEN
#                 else:
#                     try:
#                         order.provider = provider
#                         order.facility = client_facility_map['medlab2020'].get('ML01')
#                         billing_driver.yield_billing_report(order, result_timestamp)
#                     except:
#                         traceback.print_exc()
#                         ostatus |= OStatus.FAILED_TO_YIELD_BILLING_REPORT
#             except BadPHIException:
#                 print(f'Bad PHI for [{mrn}]')
#                 ostatus |= OStatus.FAILED_TO_PARSE_PHI
#             except Exception as e:
#                 ostatus |= OStatus.FAILED_TO_YIELD_BILLING_REPORT
#                 traceback.print_exc()
#             finally:
#                 ostatus &= ~OStatus.PROCESSING
#
#                 self.idx_cache.append(idx)
#                 self.ostatus_cache.append(ostatus)
#
#                 if random() < 0.05:
#                     self.processed_queue.put((copy(self.idx_cache), copy(self.ostatus_cache)))
#                     self.idx_cache = []
#                     self.ostatus_cache = []
#                 print(f'Done with <{idx}> [{mrn}] {parse_ostatus(ostatus)}')
#                 self.task_queue.task_done()
#
#
# tracker_lock = Lock()
#
#
# class Tracker(Thread):
#     def __init__(self, this_processed_queue: Queue):
#         Thread.__init__(self)
#         self.processed_queue = this_processed_queue
#
#     def run(self):
#         while True:
#             try:
#                 (idx_cache, ostatus_cache) = self.processed_queue.get()
#                 with tracker_lock:
#                     for (idx, ostatus) in zip(idx_cache, ostatus_cache):
#                         historical_cache.loc[idx, 'ostatus'] = ostatus
#                         print(f'Processed <{idx}> {parse_ostatus(ostatus)}')
#                     historical_cache.to_csv(f'remaining_records_{unix_moment}.csv')
#
#             finally:
#                 self.processed_queue.task_done()


if __name__ == '__main__':
    from primary_automation.billing.audit import result_tables

    results_df = result_tables.df

    lock = Lock()

    max_api_connection_attempt = 3
    __here__ = abspath(dirname(__file__))
    log_filepath = join(__here__, 'logs', 'fuck.log')
    logger = logging_util.get_csv_logger(log_filepath, file_level=logging.INFO)

    unix_moment = f'{time.time():.0f}'

    n_workers = 30
    buffer_size = 20

    rabbit_mq_channel = RabbitMQChannel()

    with rabbit_mq_channel as channel:
        temp_channel_name = 'temp1'
        channel.queue_declare(queue=temp_channel_name, durable=True)
        for i, result_entry in results_df.iterrows():
            mrn = result_entry['mrn']
            if re.match(r'0[0234]-\d+', mrn):
                channel.basic_publish(exchange='',
                                      routing_key=temp_channel_name,
                                      body=mrn.encode())

            break
                # method, properties, msg_body = channel.basic_get(temp_channel_name, auto_ack=False)
    #
    # task_queue = Queue(buffer_size)
    # processed_queue = Queue(buffer_size)
    # for i in range(n_workers):
    #     worker = StashDriver(task_queue, processed_queue, billing_driver)
    #     worker.daemon = True
    #     worker.name = f'Stasher_{i}'
    #     worker.start()
    #
    #     status_driver = Tracker(processed_queue)
    #     status_driver.daemon = True
    #     status_driver.name = f'Tracker_{i}'
    #     status_driver.start()
    #
    # try:
    #     bad_idx_cache = []
    #     for i, row in historical_cache[pd.isna(historical_cache['ostatus'])].iterrows():
    #         this_mrn = row['mrn']
    #         # skip_it = False
    #         # site_code, order_number = this_mrn.split('-')
    #         # if site_code == '00' and int(order_number) <= 1700000:
    #         #     bad_idx_cache.append(i)
    #         #     skip_it = True
    #         # if len(bad_idx_cache) >= 50:
    #         #     processed_queue.put((copy(bad_idx_cache), [2] * len(bad_idx_cache)))
    #         #     bad_idx_cache = []
    #         # if skip_it:
    #         #     continue
    #
    #         this_result_timestamp = row['result_timestamp']
    #         print(f'Adding {this_mrn} to the queue')
    #         task_queue.put((i, this_mrn, dateutil.parser.parse(this_result_timestamp)))
    #         time.sleep(0.01)
    # except:
    #     traceback.print_exc()
    #
    # print()
