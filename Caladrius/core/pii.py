import re
import string
from datetime import datetime
from enum import Enum, unique


@unique
class Sex(Enum):
    MALE = ('Male', 'M')
    FEMALE = ('Female', 'F')
    OTHER = ('Other', 'O')

    def __init__(self, long_name: str, abbr: str):
        self.long_name = long_name
        self.abbr = abbr

    def __eq__(self, other):
        if isinstance(other, Sex):
            return self.value == other.value
        elif isinstance(other, str):
            return self.concept_name.upper() == other.upper()
        elif isinstance(other, int):
            return self.code == other
        else:
            return False

    def __hash__(self):
        return hash(self.long_name)


@unique
class Race(Enum):
    NATIVE = ('1002-5', 'American Indian or Alaska Native')
    ASIAN = ('2028-9', 'Asian')
    BLACK = ('2054-5', 'Black or African American')
    PACIFIC_ISLANDER = ('2076-8', 'Native Hawaiian or Other Pacific Islander')
    WHITE = ('2106-3', 'White')
    OTHER = ('2131-1', 'Other Race')

    def __init__(self, race_code: str, concept_name: str):
        self.code = race_code
        self.concept_name = concept_name

    def __eq__(self, other):
        if isinstance(other, Race):
            return self.value == other.value
        elif isinstance(other, str):
            return self.concept_name.upper() == other.upper()
        elif isinstance(other, int):
            return self.code == other
        else:
            return False

    def __hash__(self):
        return hash(self.value)


parse_race = {r.concept_name.upper(): r for r in Race}


@unique
class Ethnicity(Enum):
    HISPANIC_LATINO = 'Hispanic/Latino'
    NON_HISPANIC_LATINO = 'Non-Hispanic/Latino'
    OTHER = 'Other'

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return hash(self) == hash(other)


us_states = {
    'AK': 'Alaska',
    'AL': 'Alabama',
    'AR': 'Arkansas',
    'AZ': 'Arizona',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DC': 'District of Columbia',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'IA': 'Iowa',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'MA': 'Massachusetts',
    'MD': 'Maryland',
    'ME': 'Maine',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MO': 'Missouri',
    'MS': 'Mississippi',
    'MT': 'Montana',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'NE': 'Nebraska',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NV': 'Nevada',
    'NY': 'New York',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VA': 'Virginia',
    'VT': 'Vermont',
    'WA': 'Washington',
    'WI': 'Wisconsin',
    'WV': 'West Virginia',
    'WY': 'Wyoming'
}

bad_name_chars = re.compile(r'[^\w\s-]')


class Name:  # first middle last name of orderer
    def __init__(self, fname: str, lname: str, mname: str = ''):
        self.fname = re.sub(bad_name_chars, '', str(fname)).strip()
        self.lname = re.sub(bad_name_chars, '', str(lname)).strip()
        self.mname = string.capwords(re.sub(bad_name_chars, '', str(mname))).strip()

    def get_fullname(self) -> str:
        return ' '.join([x for x in [self.fname, self.mname, self.lname] if x])

    def get_fname(self) -> str:  # return client first name
        try:
            return string.capwords(self.fname.replace(',', ''))
        except AttributeError:
            return ""

    def get_lname(self) -> str:  # return client last name
        try:
            return string.capwords(self.lname.replace(',', ''))
        except AttributeError:
            # logger.warning("Could not find last name")
            return ""

    def get_mname(self) -> str:  # return client middle name
        try:
            return string.capwords(self.mname.replace(',', ''))
        except AttributeError:
            return ""

    def get_mname_i(self) -> str:  # return just first initial of client
        if self.mname:
            return str(self.mname[0])
        else:
            return ""


def parse_name(name_in: str):
    split_name = re.split(r'\s', name_in)
    split_name = [name_part for name_part in split_name if name_part]
    return Name(fname=split_name[0], lname=split_name[1])


class Address:  # street, city, state, zip, country of orderer (will use state to see who to send csv to)
    def __init__(self, street_1, city, state, zip_code='', street_2='', country=''):
        self.street_1 = street_1
        self.street_2 = street_2
        self.city = string.capwords(city) if city else ''
        self.state = state
        self.zip = zip_code
        self.country = string.capwords(country) if country else ''

    def get_street_2(self) -> str:  # return secondary street address of patient
        return self.street_2

    def get_formatted_address(self) -> str:  # gives comment separated address of patient
        formatted_address = self.street_1
        formatted_address += (' ' + self.street_2) if self.street_2 else ''
        formatted_address += ', ' + self.get_city_state_zip()
        return formatted_address

    def get_city_state_zip(self):
        return self.city + ', ' + self.state + ' ' + self.zip


class Contact:  # phone, email, sex, ethnicity, race of orderer
    def __init__(self, phone, email, sex: Sex, ethnicity: Ethnicity, race: Race, language,
                 third_party_emails: list = None):
        self.phone = phone
        self.email = email
        self.sex = sex
        self.ethnicity = ethnicity
        self.race = race
        self.language = language
        self.third_party_emails = third_party_emails

    def get_planguage(self) -> str:
        return self.language

    def get_phone(self) -> str:
        return self.phone

    # ethnicity_dict = {
    #     'H': 1,
    #     'HISPANIC/LATINO': 1,
    #     'NH OR H': 2,
    #     'NON-HISPANIC/LATINO': 2,
    #     'U': 3,
    #     'UNKNOWN': 3
    # }

    # Nevada/Virginia/Florida format: Hispanic or Latino, Not Hispanic or Latino, Unknown
    # def get_ethnicity(self) -> str:
    #     if not self.pEthnicity:
    #         x = 3
    #     else:
    #         x = ethnicity_dict.get(self.pEthnicity.upper(), -1)
    #
    #     if x == 1:
    #         return "Hispanic or Latino"
    #     elif x == 2:
    #         return "Not Hispanic or Latino"
    #     elif x == 3:
    #         return "Unknown"
    #     else:
    #         logger.warning(f"Could not find correct ethnicity in ethnicity_dict for: {self.pEthnicity}")
    #         return "No Response"


class Billing:
    def __init__(self,
                 client_bill: bool,
                 insurance_name: str,
                 payer_name: Name ,
                 insurance_id: str, insurance_group: str,
                 subscriber_name: Name, subscriber_dob: datetime,
                 drivers_license_number: str ,
                 social_security_number: str ):
        self.client_bill = client_bill
        self.insurance_name = insurance_name
        self.payer_name = payer_name
        self.insurance_id = insurance_id
        self.insurance_group = insurance_group
        self.subscriber_name = subscriber_name
        self.subscriber_dob = subscriber_dob
        self.drivers_license_number = drivers_license_number
        self.social_security_number = social_security_number

    def get_payer_fname(self):
        if self.payer_name:
            return self.payer_name.get_fname()
        else:
            return ''

    def get_payer_lname(self):
        if self.payer_name:
            return self.payer_name.get_lname()
        else:
            return ''

    def get_subscriber_name(self):
        if self.subscriber_name:
            return self.subscriber_name.get_fullname()
        else:
            return ''
