import html
import json
from os.path import join, abspath, dirname

import pytz
from dateutil import parser
from dateutil import tz
from dateutil.parser import parse, ParserError
from dateutil.tz import gettz

from Caladrius.Covid_Clinic import get_order
from Caladrius.auxiliary import logging_util, xstr
from Caladrius.core.entities import Patient, BadPHIException
from Caladrius.core.health import Symptoms, Specimen, Order, SNOMED
from Caladrius.core.pii import Address, Contact, Name, Billing, Sex, Ethnicity, Race, parse_race

__here__ = abspath(dirname(__file__))

log_filepath = join(__here__, 'logs', 'WooCommerce.log')
# woocommerce_file_handler = TimedRotatingFileHandler(log_filepath, when='midnight', utc=True, encoding='utf-8')
# logger = logging_util.get_csv_logger(log_filepath, multithread=True, file_handler=woocommerce_file_handler)
logger = logging_util.get_csv_logger(log_filepath, multithread=True)


def get_amelia_appointment_datetime(order_json: dict):
    line_items = order_json.get('line_items')
    if line_items is None or len(line_items) == 0:
        raise ValueError('No line items---cannot find ameliabooking data')

    first_line_item_metadata = {x['key']: x['value'] for x in line_items[0]['meta_data']}
    ameliabooking = first_line_item_metadata.get('ameliabooking')

    if ameliabooking is None:
        raise ValueError('No ameliabooking data found in the first line item metadata')

    booking_end_naive_dt_str = ameliabooking.get('bookingEnd')
    booking_end_tz_str = ameliabooking.get('timeZone')

    if not booking_end_naive_dt_str:
        raise ValueError('No booking-end time')
    if not booking_end_tz_str:
        raise ValueError('No booking-end timezone')

    try:
        booking_end_naive_dt = parser.parse(booking_end_naive_dt_str)
    except parser.ParserError:
        raise ValueError(f'Could not parse "{booking_end_naive_dt_str}" as a datetime')

    booking_end_tz = tz.gettz(booking_end_tz_str)
    if not booking_end_tz:
        raise ValueError(f'"{booking_end_tz_str}" does not return a timezone')

    return booking_end_naive_dt.replace(tzinfo=booking_end_tz).astimezone(tz.gettz('utc'))


def parse_order(order_json: json) -> Order:
    try:
        # turning meta_data from array of single dictionaries to a large dictionary to index

        created_timestamp = parser.parse(order_json['date_created_gmt'] + 'Z')
        modified_timestamp = parser.parse(order_json['date_modified_gmt'] + 'Z')
        metadata = {'date_created_utc': created_timestamp,
                    'date_modified_utc': modified_timestamp}
        wc_metadata = {x['key']: x['value'] for x in order_json['meta_data']}
        line_items = order_json['line_items']
        try:
            metadata['amelia_appointment_datetime'] = get_amelia_appointment_datetime(order_json)
        except ValueError as e:
            # logger.exception(e)
            pass

        try:
            first_line_item_metadata = {x['key']: x['value'] for x in line_items[0]['meta_data']}
            facility_id = first_line_item_metadata['ameliabooking']['locationId']

            # TODO Checking to make sure it's an integer is more of a way to make sure that WooCommerce is giving me
            #  values I expect, not like facility ID actually *has* to be an integer. If we move to non-int facility
            #  IDs, I'll remove this raise block
            if not isinstance(facility_id, int):
                raise ValueError('Facility ID is expected to be parsed as an integer')

            metadata['facility_id'] = str(facility_id)
        except Exception as e:
            # logger.warning(f'Exception encountered parsing facility ID: {e}')
            # print(f'Exception encountered parsing facility ID: {e}')
            pass

        line_item_names = []
        try:
            line_item_names = [line_item['name'] for line_item in line_items]
        except Exception as e:
            logger.exception(e)

        try:
            ordered_test_name = line_items[0]['name']
            ordered_test_price = line_items[0]['price']
            metadata['ordered_test_name'] = ordered_test_name
            metadata['ordered_test_price'] = ordered_test_price
        except Exception as e:
            logger.warning(f'Exception encountered parsing ordered test name/price: {e}')

        mrn = wc_metadata["_ywbc_barcode_value"]

        # pdata will contain client information (part of response dictionary)
        pdata = order_json['patient_information']

        # getting middle initial here since it's going to be empty most of the time anyway
        name = Name(xstr(pdata['patient_first_name']),
                    xstr(pdata['patient_last_name']),
                    xstr(pdata.get('patients_middle_initial', '')))
        try:
            dob = parser.parse(pdata['patients_date_of_birth'])
        except Exception as e:
            raise BadPHIException(e)

        address = Address(street_1=(pdata['patients_address']),
                          street_2=(pdata['patients_address_2']),
                          city=(pdata['patients_city']),
                          state=xstr(pdata['patients_state']).upper(),
                          zip_code=xstr(pdata['patients_zip']),
                          country=(pdata['patients_country']))

        sex_dict = {'MALE': Sex.MALE, 'FEMALE': Sex.FEMALE}
        ethnicity_dict = {'HISPANIC/LATINO': Ethnicity.HISPANIC_LATINO,
                          'NON-HISPANIC/LATINO': Ethnicity.NON_HISPANIC_LATINO}
        race = parse_race.get(xstr(pdata['patients_race']).upper(), Race.OTHER)

        contact = Contact(xstr(pdata['patients_phone']),
                          xstr(pdata['patients_email']),
                          sex_dict.get(xstr(pdata['patients_sex']).upper(), Sex.OTHER),
                          ethnicity_dict.get(xstr(pdata['patients_ethnicity']).upper(), Ethnicity.OTHER),
                          race,
                          xstr(pdata['patients_primary_language']))

        patient = Patient(mrn, name, dob, address, contact)

        try:

            insurance_name = order_json['insurance_information']['insurance_company']
            insurance_id = order_json['insurance_information']['policy_number']
            insurance_group = order_json['insurance_information']['group_number']

            try:
                subscriber_name = Name(order_json["insurance_information"]["subscriber_first_name"],
                                       order_json["insurance_information"]["subscriber_last_name"])
                subscriber_dob = parser.parse(order_json["insurance_information"]["subscriber_dob"])
            except Exception:
                subscriber_name = name
                subscriber_dob = dob
                # logger.warn(f'Exception occurred parsing subscriber_name/dob. Defaulting to pt name/dob: {str(e)}')

            client_bill = order_json.get('billing', {}).get('client_bill')
            if client_bill is not None:
                client_bill = bool(client_bill)

            # Boiled down, this is what Ben Carfano wrote originally. If you look at the two places he would write to
            # "Client Bill.csv", it was whenever client_bill wasn't an empty string---and if it was, he'd still write
            # to it if the paid_test_flag variable was True (explained below, see conditions b) and c))
            paid_test_flag = ("150" in str(metadata.get('ordered_test_price'))) or \
                             (metadata.get('ordered_test_name') == 'Expedited RT-PCR COVID-19 Test - Next Day Results')
            # We consider it 'client billed' if any of the following conditions are met:
            # a) client bill is truthy
            # b) "150" is in the price
            # c) the test name is 'Expedited RT-PCR COVID-19 Test - Next Day Results'
            client_bill = client_bill or paid_test_flag

            try:
                drivers_license_number = order_json['insurance_information']['drivers_license_state_id'].strip()
            except Exception as e:
                drivers_license_number = None
                # logger.warning(f'Exception encountered parsing DL info: {str(e)}')

            try:
                social_security_number = order_json['insurance_information']['social_security_number'].strip()
            except Exception as e:
                social_security_number = None
                # logger.warning(f'Exception encountered parsing SSN info: {str(e)}')

            billing = Billing(client_bill=client_bill,
                              insurance_name=insurance_name,
                              payer_name=None,
                              insurance_id=insurance_id,
                              insurance_group=insurance_group,
                              subscriber_name=subscriber_name,
                              subscriber_dob=subscriber_dob,
                              drivers_license_number=drivers_license_number,
                              social_security_number=social_security_number)

            patient.billing = billing
        except Exception as e:
            patient.billing = None
            # logger.warning(f'Exception encountered parsing billing info: {str(e)}')
        # It's *ok* if the following fields fail, but we should at least log a warning
        # (as opposed to completely error-out)
        try:
            history_and_consent = order_json['history_and_consent']
            date_of_symptoms = history_and_consent.get('date_of_symptoms')
            if date_of_symptoms:
                try:
                    date_of_symptoms = parser.parse(date_of_symptoms)
                except ParserError:
                    logger.warning(f'Unable to parse date_of_symptoms "{date_of_symptoms}"')

            exposure = history_and_consent.get('exposure')
            if exposure is not None:
                if str(exposure).upper() == 'NO':
                    exposure = False
                else:
                    exposure = bool(exposure)

            symptoms_list = history_and_consent.get('symptoms_observed')
            if not symptoms_list:
                symptoms_list = None
            else:
                symptoms_list = symptoms_list.split(',')
                if not symptoms_list or symptoms_list[0] == 'None of the above':
                    symptoms_list = []

            our_symptoms = Symptoms(history_and_consent.get('insurance_status', ''),
                                    exposure,
                                    symptoms_list,
                                    date_of_symptoms,
                                    history_and_consent.get('pregnant', ''),
                                    history_and_consent.get('preop', ''))
            patient.symptoms = our_symptoms
        except Exception as e:
            patient.symptoms = None
            logger.warning(f'Exception encountered parsing symptoms info: {str(e)}')

        try:
            third_party_emails = order_json['third_party_info']['email_my_test_results_2'].strip()
            third_party_emails = third_party_emails.split(',')
            third_party_emails = [html.unescape(tpe) for tpe in third_party_emails
                                  if tpe and not tpe == contact.email]

            patient.contact.third_party_emails = third_party_emails
        except Exception as e:
            patient.contact.third_party_emails = None
            logger.warning(f'Exception encountered parsing third-party info: {str(e)}')

        try:
            patient.passport_num = order_json["travel"]["passport_number"].strip()
        except Exception:
            patient.passport_num = None
            # logger.warning(f'Exception encountered parsing passport number info: {str(e)}')
            # print(f'Exception encountered parsing passport number info: {str(e)}')

        try:
            patient.insurance_status = order_json['insurance_information']['insurance_status'].strip()
        except Exception as e:
            patient.insurance_status = None
            logger.warning(f'Exception encountered parsing insurance status info: {str(e)}')

        # Specimen
        try:
            patient.passport_country = order_json["travel"]["passport_country"].strip()
        except Exception as e:
            patient.passport_country = None
            # logger.warning(f'Exception encountered parsing passport country info: {str(e)}')

        if order_json['billing']['mt_collection_method'].upper() == 'X':
            # mid-turbinate
            specimen_source = SNOMED.MID_TURBINATE
        elif order_json['billing']['np_collection_method'].upper() == 'X':
            # Nasopharyngeal
            specimen_source = SNOMED.NASOPHARYNGEAL
        elif order_json['billing']['an_collection_method'].upper() == 'X':
            # Anterior nares
            specimen_source = SNOMED.ANTERIOR_NARES
        else:
            # Per Vicky Steele's direction (9/9/21), I am defaulting to mid-turbinate
            # logger.warning(f'{mrn} does not have a specimen collection site. Defaulting to mid-turbinate')
            # Per Vicky Steele's direction (1/7/22), I am defaulting to anterior nares
            specimen_source = SNOMED.ANTERIOR_NARES
            # in the future this should be nothing if there is actually nothing
        try:
            # Ramses Angles confirmed on 2021-10-30 that "collection date and time are in UTC"
            collection_date_str = wc_metadata['collection_date']
            collection_time_str = wc_metadata['collection_time']
            if not collection_date_str or not collection_time_str:
                raise ValueError(f'Collection date not found for {mrn}')
            collection_datetime = parser.parse(collection_date_str + ' ' + collection_time_str)
            collection_datetime = pytz.utc.localize(collection_datetime)
            collection_datetime = collection_datetime.astimezone(gettz('UTC'))
        except Exception as e:
            collection_datetime = None
            print(f'Exception encountered parsing collection datetime info: {str(e)}')
            # logger.warning(f'Exception encountered parsing collection datetime info: {str(e)}')
        testing_location = order_json['billing']['testing_location']
        specimen = Specimen(collection_datetime, specimen_source, testing_location)

        order = Order(metadata,
                      patient,
                      specimen)

        order.line_item_names = line_item_names
        return order

    except Exception as e:
        logger.critical(f'Exception occurred while parsing order: {str(e)}')
        raise


# def get_collection_time_from_notes(pt_notes):
#     if pt_notes:
#         pt_notes.raise_for_status()
#         pt_notes = [(note['note'], note['date_created_gmt']) for note in pt_notes.json()]
#         pt_notes = sorted(pt_notes, key=lambda pt_note: pt_note[1], reverse=True)
#         pt_notes_filtered = [x for x in pt_notes if 'DYMO Label printed' in x[0]]
#         if len(pt_notes_filtered) > 0:
#             label_print_timestamp_gmt = parser.parse(pt_notes_filtered[0][1])
#             label_print_timestamp = pytz.utc.localize(label_print_timestamp_gmt)
#             return label_print_timestamp.astimezone(gettz('UTC'))
#         else:
#             raise NotImplementedError(f'Unable to find "DYMO Label printed" in the order notes')


def get_latest_label_print_time(mrn: str):
    pt_notes = get_order(mrn, notes=True)
    pt_notes.raise_for_status()
    pt_notes = [(note['note'], note['date_created_gmt']) for note in pt_notes.json()]
    pt_notes = sorted(pt_notes, key=lambda pt_note: pt_note[1], reverse=True)
    pt_notes_filtered = [x for x in pt_notes if 'DYMO Label printed' in x[0]]
    if len(pt_notes_filtered) > 0:
        label_print_timestamp_gmt = parse(pt_notes_filtered[0][1])
        return pytz.utc.localize(label_print_timestamp_gmt)
    else:
        return None
