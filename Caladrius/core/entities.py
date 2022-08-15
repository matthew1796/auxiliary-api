from datetime import datetime
from os.path import dirname, abspath

from .pii import Name, Address, Contact, Billing, parse_name

__here__ = dirname(abspath(__file__))


class BadPHIException(Exception):
    pass


class Patient:
    def __init__(self, mrn: str, name: Name, dob: datetime, address: Address, contact: Contact,
                 billing: Billing = None):
        # id will be equal to the mrn
        self.id = mrn
        self.mrn = mrn
        self.name = name
        self.DOB = dob
        self.address = address
        self.contact = contact
        self.billing = billing

        self.symptoms = None
        self.third_party_email = None
        self.passport_num = None
        self.passport_country = None

        self.insurance_status = None

        self.metadata = {}

    def get_mrn(self) -> str:
        return str(self.mrn)

    def get_id(self) -> str:
        return str(self.id)

    # def get_webcode(self) -> str:
    #     return self.webcode

    def get_patient_ID(self) -> str:
        return self.mrn

    def get_accession(self) -> str:
        return self.mrn

    def get_passport_number(self) -> str:
        return self.passport_num

    def get_passport_country(self) -> str:
        return self.passport_country

    def get_age(self):
        born = self.DOB
        if not born:
            raise ValueError('DOB is null -- cannot compute age')
        today = datetime.today()
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

    def print_all(self):
        # call all the prints to display order data
        # Name/ID/Birthday/Address/Phone/Email/Sex/Race/Ethnicity
        print(f'\n\nFirst Name: {self.name.get_fname()}')
        print(f'Last Name: {self.name.get_lname()}')
        print(f'ID: {self.get_id()}')
        print(f"Date of Birth: {self.DOB.isoformat()}")
        print(f'Address: {self.address.get_formatted_address()}')
        print(f'Phone #: {self.contact.get_phone()}')
        print(f'Email: {self.contact.email}')
        print(f'Sex: {self.contact.sex}')
        print(f'Race: {self.contact.race}')
        print(f'Ethnicity: {self.contact.ethnicity}')
        # Accessioning number/Collection Time/Date/Collection site/Facility/ Provider/Provider Name
        print(f'Accessioning number: {self.get_accession()}')

        print(f'Third-party: {self.contact.third_party_emails}')
        print(f'Passport: {self.get_passport_number()}')
        print(f'Passport: {self.get_passport_country()}')


class Facility:
    def __init__(self, director: str, lab_id: str, f_name: str, f_address: Address,
                 f_phone: str, f_clia: str, f_state_id: str, time_zone: str = None):
        self.director = director
        self.lab_id = lab_id
        self.name = f_name
        self.address = f_address
        self.phone = f_phone
        self.clia = f_clia
        self.state_id = f_state_id
        self.time_zone = time_zone


class Provider:  # Info about provider (not MedLab2020)
    def __init__(self, facility_name: str, doctor_name: str, doctor_npi: str, address: Address, phone: str):
        self.facility_name = facility_name
        self.doctor_name = parse_name(doctor_name)
        self.npi = doctor_npi
        self.address = address
        self.phone = phone
