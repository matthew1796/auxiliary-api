import hashlib
import json
import logging
import traceback
from datetime import datetime
from os.path import abspath, dirname, join

import pymongo.collection
import pytz
import requests
from dateutil import tz
from dateutil.parser import parse

from Caladrius.Covid_Clinic import ccadmin, WooCommerce, source_woocommerce, source_ccadmin, wc_api, loginutil
from Caladrius.DocumentDB import MongoClient

# MongoDB Archive

__here__ = abspath(dirname(__file__))

from Caladrius.auxiliary import logging_util, mkdir
from Caladrius.core.health import Order

log_filepath = join(mkdir(join(__here__, 'logs')), 'cc_db.log')
logger = logging_util.get_csv_logger(log_filepath, file_level=logging.INFO, multithread=True)


def utcdt(dt: datetime):
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=tz.gettz('utc'))
    else:
        dt = dt.astimezone(tz=tz.tzutc())

    return dt


def has_good_cx_dt(order: Order, ax_dt: datetime):
    if (order.specimen is not None) and (order.specimen.collection_datetime is not None):
        cx_dt = utcdt(order.specimen.collection_datetime)
        ax_dt = utcdt(ax_dt)
        return cx_dt <= ax_dt

    return False


def get_limited_DYMO_label_print_time(notes, label_timestamp_threshold):
    pt_notes = [(note['note'], note['date_created_gmt']) for note in notes]
    pt_notes = sorted(pt_notes, key=lambda pt_note: pt_note[1], reverse=True)
    pt_label_times_filtered = [x[1] for x in pt_notes if 'DYMO Label printed' in x[0]]

    times_limited = [x for x in pt_label_times_filtered if x <= label_timestamp_threshold.isoformat()]
    if times_limited:
        label_print_time = parse(times_limited[0])
        label_print_time = pytz.utc.localize(label_print_time)
        return label_print_time
    else:
        return None


def get_order_notes_json(db_client: pymongo.MongoClient, mrns: str , force_update=False) -> dict:
    notes_collection = db_client['covidclinic']['notes']
    if isinstance(mrns, str):
        mrns = [mrns]

    retrieved_notes = notes_collection.find({'mrn': {'$in': mrns}}, sort=[('date_created', 1)])
    retrieved_notes = {x['mrn']: x for x in retrieved_notes}

    notes_to_insert = []
    for mrn in mrns:
        retrieved_note = retrieved_notes.get(mrn)

        raw_body_hash = None
        actually_pull = False
        if retrieved_note is None:
            #logger.debug(f'No notes data found for mrn=[{mrn}]. Attempting to retrieve and stash...')
            actually_pull = True
        elif force_update:
            #logger.debug(f'Force updating notes data for mrn=[{mrn}]. Attempting to retrieve and stash...')
            actually_pull = True
            raw_body_hash = retrieved_note['md5(raw_body)']

        if actually_pull:
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
                else:
                    raise ValueError(f'Unexpected site_code "{site_code}". Could not deduce source_of_truth.')

                notes_response = None
                if source_of_truth == source_woocommerce:
                    notes_response = wc_api[stage][site_code].get(f"orders/{order_number}/notes")
                    if notes_response.status_code == 404 and site_code == '00':
                        stage = 'archive'
                        notes_response = wc_api[stage][site_code].get(f"orders/{order_number}/notes")
                    notes_response.close()

                if not notes_response:
                    raise ValueError(f'No notes for [{mrn}]')

                notes_body = notes_response.json()

                if notes_body is None:
                    raise ValueError(f'Notes payload body for [{mrn}] is None')

                note_to_insert = {
                    'mrn': mrn,
                    'source_of_truth': source_of_truth,
                    'stage': stage,
                    'raw_body': notes_body,
                    'md5(raw_body)': hashlib.md5(
                        json.dumps(notes_body, sort_keys=True).encode()).hexdigest(),
                    'date_created': datetime.utcnow().isoformat()
                }
                if raw_body_hash == note_to_insert['md5(raw_body)']:
                    logger.debug(f'md5(raw_body) of [{mrn}] order notes already stashed.')
                else:
                    notes_to_insert.append(note_to_insert)
                    retrieved_notes[mrn] = note_to_insert

            except Exception as e:
                logger.exception(e)
                traceback.print_exc()

    if notes_to_insert:
        insert_many_result = notes_collection.insert_many(notes_to_insert)
        if not insert_many_result.acknowledged:
            raise ValueError(f'Insertion of mrns=[{mrns}] into notes not acknowledged')

        for _id, note_to_insert in zip(insert_many_result.inserted_ids, notes_to_insert):
            mrn = note_to_insert['mrn']
            retrieved_notes[mrn]['_id'] = _id

    return retrieved_notes


def get_order_json(db_client: pymongo.MongoClient, mrns: str , force_update=False) -> dict:
    orders_collection = db_client['covidclinic']['orders']
    if isinstance(mrns, str):
        mrns = [mrns]

    try:
        retrieved_docs = orders_collection.find({'mrn': {'$in': mrns}}, sort=[('date_created', 1)])
        retrieved_docs = {x['mrn']: x for x in retrieved_docs}
    except:
        traceback.print_exc()
        retrieved_docs = {}

    docs_to_insert = []
    for mrn in mrns:
        retrieved_doc = retrieved_docs.get(mrn)

        raw_body_hash = None
        actually_pull = False
        if retrieved_doc is None:
            logger.debug(f'No order data found for mrn=[{mrn}]. Attempting to retrieve and stash...')
            actually_pull = True
        elif force_update:
            logger.debug(f'Force updating order data for mrn=[{mrn}]. Attempting to retrieve and stash...')
            actually_pull = True
            raw_body_hash = retrieved_doc['md5(raw_body)']

        if actually_pull:
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
                else:
                    raise ValueError(f'Unexpected site_code "{site_code}". Could not deduce source_of_truth.')

                order_response = None
                if source_of_truth == source_woocommerce:
                    order_response = wc_api[stage][site_code].get(f"orders/{order_number}")
                    if order_response.status_code == 404 and site_code == '00':
                        stage = 'archive'
                        order_response = wc_api[stage][site_code].get(f"orders/{order_number}")
                    order_response.close()

                if not order_response:
                    source_of_truth = source_ccadmin
                    stage = 'production'

                if source_of_truth == source_ccadmin:
                    with requests.Session() as sesh:
                        token = loginutil.get_token(60 * 12)
                        sesh.headers.update({'Authorization': 'Bearer {}'.format(token)})
                        order_response = sesh.get('https://api.covidclinic.org/api/v1/storefront',
                                                  params={'mrn': mrn})
                        order_response.close()

                if order_response is None:
                    raise ValueError(f'Order response for [{mrn}] is None')
                order_body = order_response.json()

                if order_body is None:
                    raise ValueError(f'Order JSON body for [{mrn}] is None')

                if (source_of_truth == source_ccadmin) and not order_body.get('total'):
                    raise ValueError(f'CCAdmin returned no results for [{mrn}]')

                doc_to_insert = {
                    'mrn': mrn,
                    'source_of_truth': source_of_truth,
                    'stage': stage,
                    'raw_body': order_body,
                    'md5(raw_body)': hashlib.md5(json.dumps(order_body, sort_keys=True).encode()).hexdigest(),
                    'date_created': datetime.utcnow().isoformat()
                }

                if raw_body_hash == doc_to_insert['md5(raw_body)']:
                    logger.debug(f'md5(raw_body) of [{mrn}] order already stashed.')
                else:
                    docs_to_insert.append(doc_to_insert)
                    retrieved_docs[mrn] = doc_to_insert

            except Exception as e:
                logger.exception(e)
                traceback.print_exc()
                retrieved_docs[mrn] = None

    try:
        if docs_to_insert:
            insert_many_result = orders_collection.insert_many(docs_to_insert)
            if not insert_many_result.acknowledged:
                raise ValueError(f'Insertion of mrns=[{mrns}] into orders not acknowledged')
            else:
                logger.debug(f"Inserted {[x['mrn'] for x in docs_to_insert]}")

            for _id, doc_to_insert in zip(insert_many_result.inserted_ids, docs_to_insert):
                mrn = doc_to_insert['mrn']
                retrieved_docs[mrn]['_id'] = _id
    except:
        traceback.print_exc()

    return retrieved_docs


def get_cc_order(mrns: str , ax_dts: datetime  = None,
                 this_db_client: pymongo.MongoClient = None, force_update=False, amend_collection_timestamp=True):
    if this_db_client is None:
        close_db_client_on_leave = True
        this_db_client = MongoClient('master')
    else:
        close_db_client_on_leave = False

    if isinstance(mrns, str):
        return_single_object = True
        mrns = [mrns]
    else:
        return_single_object = False

    if len(set(mrns)) != len(mrns):
        logger.warning('Undocumented behavior for order retrievals with duplicate MRNs')

    if ax_dts is None:
        ax_dts = [pytz.utc.localize(datetime.utcnow())] * len(mrns)
    elif isinstance(ax_dts, datetime):
        ax_dts = [ax_dts] * len(mrns)
    elif isinstance(ax_dts, list) and len(ax_dts) != len(mrns):
        raise ValueError('List of accession thresholds must align with list of MRNs')

    retrieved_orders = {mrn: None for mrn in mrns}
    db_client = None
    try:
        db_client = this_db_client.__enter__()
        # with this_db_client as db_client:
        retrieved_docs = get_order_json(db_client, mrns, force_update)
        for mrn, retrieved_doc in retrieved_docs.items():
            if retrieved_doc is None:
                logger.error(f'No order document found for mrn=[{mrn}]')
                continue
            source_of_truth = retrieved_doc['source_of_truth']
            stage = retrieved_doc['stage']
            order = None
            if source_of_truth == 'ccadmin':
                storefront_results = retrieved_doc['raw_body']['results']
                if not storefront_results:
                    logger.debug(f'No ccadmin data found for mrn=[{mrn}]')
                else:
                    storefront_result = storefront_results[0]
                    try:
                        order = ccadmin.parse_order(storefront_result)
                    except Exception:
                        traceback.print_exc()
            elif source_of_truth == 'woocommerce':
                if stage == 'archive':
                    #logger.debug('WooCommerce archive parser not implemented...')
                    order = WooCommerce.parse_archive_order(retrieved_doc['raw_body'])
                else:
                    try:
                        order = WooCommerce.parse_order(retrieved_doc['raw_body'])
                    except Exception:
                        traceback.print_exc()
            retrieved_orders[mrn] = order

        if amend_collection_timestamp:
            # Per Matt Collins 2022-06-21, only check date_modified for collection_date... smh
            # 02-1114728 is an example of a sample that will get messed up by this
            # Per Phil DuMas 2022-06-29, check:
            # 1. Collection date
            # 2. Date modified
            # 3. Notes
            # 4. Booking date
            set_cx_dt_by_notes = []
            # set_cx_dt_by_modified = []
            for ax_dt, (mrn, order) in zip(ax_dts, retrieved_orders.items()):
                if order is None:
                    continue
                if not has_good_cx_dt(order, ax_dt):
                    date_modified_utc = order.metadata['date_modified_utc']
                    logger.debug(f'Amending [{mrn}] collection timestamp to date_modified_utc {date_modified_utc}')
                    order.specimen.collection_datetime = date_modified_utc

                    if not has_good_cx_dt(order, ax_dt):
                        set_cx_dt_by_notes.append((order, ax_dt))

            set_cx_dt_by_appt_time = []
            if set_cx_dt_by_notes:
                mrns = [x[0].patient.mrn for x in set_cx_dt_by_notes]
                retrieved_notes = get_order_notes_json(db_client, mrns, force_update)
                for order, ax_dt in set_cx_dt_by_notes:
                    mrn = order.patient.mrn
                    retrieved_note = retrieved_notes.get(mrn)
                    if retrieved_note is None:
                        logger.debug(f'No notes retrieved for mrn=[{mrn}]')
                        continue
                    label_print_dt = get_limited_DYMO_label_print_time(retrieved_note['raw_body'], ax_dt)
                    logger.debug(f'Amending [{mrn}] collection timestamp to label_print_dt {label_print_dt}')
                    order.specimen.collection_datetime = label_print_dt

                    if not has_good_cx_dt(order, ax_dt):
                        set_cx_dt_by_appt_time.append((order, ax_dt))
                    # if label_print_dt is not None and label_print_dt <= ax_dt:
                    #     logger.debug(f'Amending [{mrn}] collection timestamp to label print-time {label_print_dt}')
                    #     retrieved_orders[mrn].specimen.collection_datetime = label_print_dt
                    # else:
                    #     logger.warning(f'Unable to amend [{mrn}] collection timestamp by label print-time')
                    #     set_cx_dt_by_appt_time.append((mrn, ax_dt))

            for order, ax_dt in set_cx_dt_by_appt_time:
                mrn = order.patient.mrn
                amelia_appointment_datetime = order.metadata.get('amelia_appointment_datetime')
                logger.debug(f'Amending [{mrn}] collection timestamp '
                             f'to amelia appointment datetime {amelia_appointment_datetime}')
                order.specimen.collection_datetime = amelia_appointment_datetime

                if not has_good_cx_dt(order, ax_dt):
                    logger.warning(f'Unable to amend [{mrn}] collection timestamp')

    finally:
        if close_db_client_on_leave and (db_client is not None):
            db_client.close()

    # Preflight check, for sanity
    for mrn, order in retrieved_orders.items():
        if mrn and order:
            if order.patient.mrn != mrn:
                logger.critical(f'!!!! MRN mismatch! Order object MRN [{order.patient.mrn}] != {mrn}!')
                retrieved_orders[mrn] = None

    if return_single_object:
        return list(retrieved_orders.values())[0]
    return retrieved_orders


if __name__ == '__main__':
    # 00-3995083 is an example of a WC order that has multiple dymo label print times
    # 02-1237683 is an example of a WC order that has multiple dymo label print times, like a lot, and they're all on
    # the same day smh

    # with MongoClient('master') as my_db_client:
    #     x = get_order_json(my_db_client, ['00-5076626', '00-5075157', '00-5076808'])
    # my_orders = get_cc_order(['00-5076626', '00-5075157', '00-5076808'], force_update=True)
    my_orders = get_cc_order('02-1114728', parse('2021-11-14T17:07:05'), force_update=True)
    print(my_orders)
    print()
