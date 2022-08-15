import contextlib
import json
import re
from datetime import datetime, timedelta as td, timezone as tz
from logging.handlers import TimedRotatingFileHandler
from os.path import abspath, dirname, join, exists

import jwt
import requests as req
from dateutil import parser

from Caladrius.auxiliary import logging_util, mkdir
from Caladrius.core import health
from Caladrius.core import pii
from Caladrius.core.entities import Patient, BadPHIException
from Caladrius.core.health import Specimen, Order
from Caladrius.core.health import Symptoms
from Caladrius.core.helper import xstr
from Caladrius.core.pii import Address, Contact, Name, Billing, Sex, Ethnicity

__here__ = abspath(dirname(__file__))

log_filepath = join(mkdir(join(__here__, 'logs')), 'ccadmin.log')
# ccadmin_file_handler = TimedRotatingFileHandler(log_filepath, when='midnight', utc=True, encoding='utf-8')
# logger = logging_util.get_csv_logger(log_filepath, multithread=True, file_handler=ccadmin_file_handler)
logger = logging_util.get_csv_logger(log_filepath, multithread=True)


def parse_order(storefront_result: dict) -> Order:
    try:
        date_modified_utc = parser.parse(xstr(storefront_result.get('last_modified')))
    except Exception as e:
        date_modified_utc = None

    metadata = {'date_created_utc': None,
                'date_modified_utc': date_modified_utc}

    mrn = storefront_result['mrn']
    name = Name(xstr(storefront_result['first_name']).capitalize(),
                xstr(storefront_result['last_name']).capitalize(),
                xstr(storefront_result.get('middle_initial', '')).capitalize())

    line_item_names = []
    try:
        line_items = storefront_result['order_data'].get('line_items')
        if isinstance(line_items, list):
            line_item_names = [line_item['product_name'] for line_item in line_items]
        else:
            logger.warning(f'No line items found for mrn=[{mrn}]')
    except Exception as e:
        logger.exception(e)

    try:
        dob = parser.parse(storefront_result['date_of_birth'])
    except Exception as e:
        raise BadPHIException(e)

    address = Address(street_1=xstr(storefront_result['street']),
                      city=xstr(storefront_result['city']),
                      state=xstr(storefront_result['state']).upper(),
                      zip_code=xstr(storefront_result['zip']),
                      street_2='',
                      country='')

    sex_dict = {'MALE': Sex.MALE, 'FEMALE': Sex.FEMALE}
    ethnicity_dict = {'HISPANIC/LATINO': Ethnicity.HISPANIC_LATINO,
                      'NON-HISPANIC/LATINO': Ethnicity.NON_HISPANIC_LATINO}
    race = pii.parse_race.get(xstr(storefront_result['race']).upper(), pii.Race.OTHER)

    contact = Contact(xstr(storefront_result['phone']),
                      xstr(storefront_result['email']),
                      sex_dict.get(xstr(storefront_result['sex']).upper(), Sex.OTHER),
                      ethnicity_dict.get(xstr(storefront_result['ethnicity']).upper(), Ethnicity.OTHER),
                      race,
                      xstr(storefront_result['language']))

    patient = Patient(mrn, name, dob, address, contact)
    patient.metadata['school_district'] = xstr(storefront_result.get('district', None))
    patient.metadata['school_name'] = xstr(storefront_result.get('school', None))

    try:
        client_bill = storefront_result.get('client_bill')
        if client_bill is not None:
            client_bill = bool(client_bill)

        billing = Billing(client_bill=client_bill,
                          insurance_name=xstr(storefront_result['primary_insurance_company_name']),
                          payer_name=None,
                          insurance_id=xstr(storefront_result['primary_insurance_policy_number']),
                          insurance_group=xstr(storefront_result['primary_insurance_plan_id']),
                          subscriber_name=None,
                          subscriber_dob=None,
                          drivers_license_number=storefront_result['driver_license'],
                          social_security_number=storefront_result['ssn'])

        patient.billing = billing
    except Exception as e:
        patient.billing = None
        logger.warning(f'Exception encountered parsing billing info: {str(e)}')

    try:
        exposure = storefront_result.get('exposure')
        if exposure is not None:
            exposure = bool(exposure)

        symptoms_list = storefront_result.get('symptons')
        if not symptoms_list:
            symptoms_list = None
        else:
            symptoms_list = symptoms_list.split(',')
            if not symptoms_list or symptoms_list[0] == 'None of the above':
                symptoms_list = None

        patient.symptoms = Symptoms(xstr(storefront_result.get('insurance_status', '')),
                                    exposure,
                                    symptoms_list,
                                    xstr(storefront_result.get('date_of_symptoms', '')),
                                    xstr(storefront_result.get('pregnant', '')),
                                    xstr(storefront_result.get('preop', None)))
    except Exception as e:
        patient.symptoms = None
        logger.warning(f'Exception encountered parsing symptoms info: {str(e)}')

    try:
        patient.contact.third_party_email = storefront_result['third_party']
    except Exception as e:
        patient.contact.third_party_email = None
        logger.warning(f'Exception encountered parsing third-party info: {str(e)}')

    try:
        patient.passport_num = storefront_result["passport_number"]
    except Exception as e:
        patient.passport_num = None
        logger.warning(f'Exception encountered parsing passport number info: {str(e)}')

    try:
        patient.passport_country = storefront_result["passport_country"]
    except Exception as e:
        patient.passport_country = None
        logger.warning(f'Exception encountered parsing passport country info: {str(e)}')

    try:
        patient.insurance_status = storefront_result["insurance_status"]
    except Exception as e:
        patient.insurance_status = None
        logger.warning(f'Exception encountered parsing insurance status info: {str(e)}')

    # Specimen
    try:
        collection_timestamp = parser.parse(xstr(storefront_result['collection_date']))
    except Exception as e:
        collection_timestamp = None
        # logger.warning(f'Exception encountered parsing collection timestamp info: {str(e)}')
    specimen_src = re.sub('[^a-zA-Z]', '', xstr(storefront_result['collection_method'])).lower()
    if specimen_src == 'anteriornares':
        specimen_src = health.SNOMED.ANTERIOR_NARES
    elif specimen_src == 'nasopharyngeal':
        specimen_src = health.SNOMED.NASOPHARYNGEAL
    elif specimen_src == 'midturbinate':
        specimen_src = health.SNOMED.MID_TURBINATE
    elif specimen_src == 'saliva':
        specimen_src = health.SNOMED.SALIVA
    else:
        # Per Vicky Steele's direction (9/9/21), I am defaulting to mid-turbinate
        # logger.warning(f'{mrn} does not have a specimen collection site. Defaulting to mid-turbinate')
        # specimen_src = 'Mid-turbinate'
        # Per Vicky Steele's direction (1/7/22), I am defaulting to anterior nares
        specimen_src = health.SNOMED.ANTERIOR_NARES

    testing_location = xstr(storefront_result['testing_location'])

    specimen = Specimen(collection_timestamp, specimen_src, testing_location)

    order = Order(metadata, patient, specimen)
    order.line_item_names = line_item_names

    return order


login_url = 'https://api.covidclinic.org/login'
token_filepath = join(dirname(abspath(__file__)), 'token.json')


class LoginUtil:
    def __init__(self):
        self.token = None
        self.read_token_from_file()
        with open(join(dirname(abspath(__file__)), 'ccadmin_authentication.json'), 'r') as authentication_file:
            self.login_payload = json.load(authentication_file)

    def login(self, expires_soon=0):
        if not self.has_good_token(expires_soon):
            login_response = req.post(login_url, json=self.login_payload, headers={'Content-Type': 'application/json'})
            with open(token_filepath, 'w') as token:
                json.dump(login_response.json(), token)

        self.read_token_from_file()

    def read_token_from_file(self):
        if not exists(token_filepath):
            self.token = None
        else:
            with open(token_filepath, 'r') as token_file:
                try:
                    token_json = json.load(token_file)
                    self.token = token_json['Token']
                except KeyError:
                    self.token = None

    def has_good_token(self, expires_soon=0):
        if not self.token:
            return False
        else:
            token_decoded = jwt.decode(self.token, options={"verify_signature": False})
            exp = datetime.fromtimestamp(token_decoded['exp'], tz=tz.utc)
            return exp > (datetime.now(tz=tz.utc) + td(minutes=expires_soon))

    def get_token(self, expires_soon=0):
        if not self.has_good_token(expires_soon):
            self.login(expires_soon)

        return self.token


loginutil = LoginUtil()


@contextlib.contextmanager
def get_sesh():
    token = loginutil.get_token()
    with req.Session() as sesh:
        try:
            sesh.headers.update({'Authorization': 'Bearer {}'.format(token)})
            yield sesh
        finally:
            sesh.close()
            del sesh
