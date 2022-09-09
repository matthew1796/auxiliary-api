import csv
import logging
import re
import string
from datetime import datetime, timedelta
from os.path import getsize, isfile
from os.path import join as at, basename, dirname, abspath
from threading import Lock
from xml.etree import ElementTree as ET

import paramiko
import pytz

from Caladrius.auxiliary import mkdir
from Caladrius.core.health import Order
from Caladrius.core.helper import xname, xdt

__here__ = dirname(abspath(__file__))

billing_storage = r'P:\billing_reports'
billing_archive = r'P:\billing_reports\archive'

billing_epoch = datetime(year=2021, month=6, day=28)

bad_char_pattern = re.compile(r'[,]')

with open(r'Caladrius\billing\Headers.csv',
          'a+', newline='', encoding='utf-8') as headers_file:
    for row in csv.reader(headers_file):
        BILLING_HEADER = row
        break


def compute_current_billing_number(when: datetime = datetime.now()):
    week, day = divmod((when - billing_epoch).days, 7)
    billing_report_number = week * 2
    billing_report_number += 1 if day < 3 else 2
    return billing_report_number


def compute_billing_range(billing_ordinal: int):
    week, a = divmod(billing_ordinal - 1, 2)
    billing_week = billing_epoch + timedelta(weeks=week)
    return (billing_week + a * timedelta(days=3),
            billing_week + timedelta(days=2) + a * timedelta(days=4))


class BillingDriver:
    def __init__(self, lock: Lock, logger_name='billing_driver', storage=None):
        self.__root_storage__ = billing_storage if storage is None else storage
        self.__current_storage__ = None
        self.lock = lock
        self.logger = logging.getLogger(logger_name)
        self.accession_suffix = '-09'

    def yield_billing_report(self, order: Order, result_timestamp: datetime,
                             icd_10_override: list = None,
                             cpt_override: list = None):
        result_timestamp = result_timestamp.astimezone(pytz.utc)

        pt = order.patient
        specimen = order.specimen
        billing = pt.billing

        if not billing:
            raise ValueError(f'Unable to generate billing data for {pt.get_mrn()}: Billing data is falsy')

        week, day = divmod((datetime.today() - billing_epoch).days, 7)
        billing_report_number = week * 2
        billing_report_number += 1 if day < 3 else 2
        self.__current_storage__ = at(self.__root_storage__, f'billing_{billing_report_number}')
        mkdir(self.__current_storage__)

        destination = 'Billing Report.csv'
        if pt.mrn.startswith('02-'):
            destination = 'Mobile Billing.csv'
        elif pt.mrn.startswith('03-'):
            destination = 'School Billing.csv'
        elif billing.client_bill:
            # If it's not 02 and not 03, then check if it's client bill (effectively 00 client billed tests)
            destination = 'Client Bill.csv'

        collection_timestamp = specimen.get_collection_datetime()

        if collection_timestamp:
            collection_timestamp_str = collection_timestamp.strftime('%m/%d/%Y')
        else:
            raise ValueError(f'Collection timestmap not found for [{pt.mrn}]')

        if cpt_override is None:
            cpt_codes = ['U0003']
            if specimen.collection_datetime:
                cx_date = specimen.collection_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
                rx_date = result_timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
                if (rx_date - cx_date).days <= 2:
                    cpt_codes.append('U0005')
        else:
            cpt_codes = cpt_override

        if isinstance(icd_10_override, list):
            icd_10 = ' '.join(icd_10_override)
        else:
            if pt.symptoms.exposure or pt.symptoms.symptoms or pt.symptoms.preop:
                icd_10 = 'Z20.822'
            else:
                icd_10 = 'Z11.52'

        subscriber_name = xname(billing.subscriber_name)
        subscriber_dob = xdt(billing.subscriber_dob, '%m/%d/%Y')
        if not subscriber_name and not subscriber_dob:
            subscriber_name = pt.name.get_fullname()
            subscriber_dob = pt.DOB.strftime('%m/%d/%Y')

        billing_row = {
            'Patient First Name': pt.name.get_fname(),
            'Patient Last Name': pt.name.get_lname(),
            'Patient Street': pt.address.street_1,
            'Patient City': pt.address.city,
            'Patient State': pt.address.state,
            'Patient Zip': pt.address.zip,
            'Patient DOB': pt.DOB.strftime('%m/%d/%Y'),
            'Patient Sex': string.capwords(pt.contact.sex.long_name),
            'Patient Phone Number': pt.contact.get_phone(),
            'Insurance Name': billing.insurance_name,
            'Payer of Insurance First Name': billing.get_payer_fname(),
            'Payer of Insurance Last Name': billing.get_payer_lname(),
            'Insurance ID Number': billing.insurance_id,
            'Insurance Group Number': billing.insurance_group,
            'Subscriber Name': subscriber_name,
            'Subscriber DOB': subscriber_dob,
            'Ordering Provider': order.provider.doctor_name.get_fullname(),
            'Ordering Provider NPI': order.provider.npi,
            'Accession Number': pt.mrn + self.accession_suffix,
            'Facility Street': order.facility.address.street_1,
            'Facility City': order.facility.address.city,
            'Facility State': order.facility.address.state,
            'Facility Zip': order.facility.address.zip,
            'Date of Collection': collection_timestamp_str,
            'Date of Results': result_timestamp.strftime('%m/%d/%Y'),
            'Billing CPT Code': ' '.join(cpt_codes),
            'ICD 10 Code': icd_10,
            'Facility ID': order.metadata.get('facility_id', ''),
            'Patient Drivers License': pt.billing.drivers_license_number,
            'Patient Social Security Number': pt.billing.social_security_number  # Intentionally we write nothing
        }

        billing_row = {k: re.sub(bad_char_pattern, '', '' if v is None else v) for k, v in billing_row.items()}

        with self.lock:
            report_filepath = at(self.__current_storage__, destination)
            with open(report_filepath, 'a', newline='') as f:
                w = csv.DictWriter(f, fieldnames=list(billing_row.keys()))
                if not isfile(report_filepath) or getsize(report_filepath) == 0:
                    w.writeheader()
                    f.flush()
                w.writerow(billing_row)


# sftp_xml = ET.parse(at(__here__, 'mpp_sftp.xml'))
# root_element = sftp_xml.getroot()
# host = root_element.findtext('host')
# username = root_element.findtext('username')
# private_key = root_element.findtext('private_key')
#
# ssh = paramiko.SSHClient()
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
# key = paramiko.RSAKey.from_private_key_file(private_key)


def upload_file(in_path: str, out_path: str, overwrite=False):
    ssh.connect(host, username=username, pkey=key)
    sftp = ssh.open_sftp()
    out_path = at(out_path, basename(in_path))
    exists = False
    try:
        sftp.stat(out_path)
        exists = True
        print(f'{out_path} exists')
    except IOError:
        pass

    if (not exists) or (exists and overwrite):
        sftp.put(in_path, out_path)
