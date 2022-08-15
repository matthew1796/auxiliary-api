from datetime import datetime
from enum import Enum, unique
from typing import Union

from Caladrius.auxiliary import xstr
from Caladrius.core.entities import Patient, Facility, Provider


class Symptoms:
    def __init__(self, insurance_status, exposure: bool, symptoms: list, symptom_date: datetime, pregnant, preop):
        self.insurance_status = insurance_status
        self.exposure = exposure
        self.pregnant = pregnant
        self.symptoms = symptoms
        self.symptom_date = symptom_date
        self.preop = preop

    def is_pregnant(self):
        return self.pregnant

    def get_exposed(self):
        return self.exposure


@unique
class LOINC(Enum):
    SARS_COV_2_RNA_NAA = \
        ('94500-6',
         'SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection',
         'SARS-CoV-2 RNA Resp Ql NAA+probe')

    SARS_COV_2_ORF1ab_RNA_NAA = \
        ('94559-2',
         'SARS-CoV-2 (COVID-19) ORF1ab region [Presence] in Respiratory specimen by NAA with probe detection',
         'SARS-CoV-2 ORF1ab Resp Ql NAA+probe')

    def __init__(self, loinc_code, long_common_name, short_name):
        self.loinc_code = loinc_code
        self.long_common_name = long_common_name
        self.short_name = short_name

    def __hash__(self):
        return hash((self.loinc_code, self.long_common_name))


@unique
class SNOMED(Enum):
    NOT_DETECTED = (260415000, 'Not detected')
    DETECTED = (260373001, 'Detected')
    INDETERMINATE = (82334004, 'Indeterminate')
    INVALID_RESULT = (455371000124106, 'Invalid result')

    NASOPHARYNGEAL = (258500001, 'Nasopharyngeal swab', 'Nasopharyngeal')
    ANTERIOR_NARES = (697989009, 'Anterior nares swab', 'Anterior nares')
    MID_TURBINATE = (871810001, 'Mid-turbinate nasal swab', 'Mid-turbinate')
    SALIVA = (119342007, 'Saliva specimen', 'Saliva')

    def __init__(self, snomed_code: int, concept_name: str, alt_name: str = None):
        self.code = snomed_code
        self.concept_name = concept_name
        self.alt_name = alt_name

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __hash__(self):
        return hash((self.code, self.concept_name))


parse_snomed = {s.concept_name.upper(): s for s in SNOMED}


class Result:
    def __init__(self, snomed: Union[SNOMED, str], result_timestamp: datetime):
        if isinstance(snomed, str):
            self.snomed = parse_snomed[snomed.upper()]
        elif isinstance(snomed, SNOMED):
            self.snomed = snomed
        self.timestamp = result_timestamp

    def __eq__(self, other):
        return hash(self) == hash(other)
        # if isinstance(other, Result):
        #     return (self.snomed == other.snomed) and (self.timestamp == other.timestamp)
        # elif isinstance(other, SNOMED):
        #     return self.snomed == other
        # else:
        #     return False

    def __hash__(self):
        return hash((self.snomed, self.timestamp))


class Specimen:  # information about covid collection: collectionDate and collectionTime
    def __init__(self, collection_timestamp: datetime,
                 specimen_source: SNOMED,
                 testing_location='',
                 received_datetime: datetime = None):
        self.specimen_source = specimen_source
        self.collection_datetime = collection_timestamp
        self.testing_location = testing_location
        self.received_datetime = received_datetime

        # might not be hardcoded in the future
        self.test_loinc = LOINC.SARS_COV_2_RNA_NAA
        self.test_id = "94500-6"
        self.test_name = "SARS-CoV-2 (COVID-19) RNA [Presence] in Respiratory specimen by NAA with probe detection"
        self.set_vendorSpecimenDescription()
        self.set_test_name()
        self.az_device_name = "QuantiVirus SARS-CoV-2 Test kit"

    def set_test_name(self):  # return result test name based on test_id
        if self.test_id == "94500-6":
            self.test_name = "SARS coronavirus 2 RNA [Presence] in Respiratory specimen by NAA with probe detection"
            self.test_speed = "Non-Rapid"
            self.set_test_unit()
        else:
            self.test_name = ""
            self.test_speed = ""
            self.set_test_unit()

    def set_test_unit(self):  # return test notation based on test_id
        if self.test_id == "94500-6":
            self.test_unit = "LN"
        else:
            self.test_unit = ""

    def set_vendorSpecimenDescription(self):  # return array of SCT values based on specimen_source
        src = xstr(self.specimen_source).upper()
        if src == "NASOPHARYNGEAL":
            self.vendorSpecimenDescription = ["258500001", "Nasopharyngeal swab", "SCT"]
        elif src == "ANTERIOR NARES":
            self.vendorSpecimenDescription = ["697989009", "Anterior nares swab", "SCT"]
        elif src == "MID-TURBINATE":
            self.vendorSpecimenDescription = ["871810001", "Mid-turbinate nasal swab", "SCT"]
        else:
            # logger.debug("There is no valid specimen source present, defaulting to nothing")
            self.vendorSpecimenDescription = []

    def get_test_speed(self):
        return self.test_speed

    def get_vendorSpecimenDescription(self, x):  # access SCT array
        if self.vendorSpecimenDescription:
            return self.vendorSpecimenDescription[x]
        return ""

    def get_device_name(self):  # Get the name of the testing kid
        return self.az_device_name

    # called 2 different ones on accident
    def get_specimen_source(self):
        return self.specimen_source

    def get_collection_datetime(self) -> datetime:
        return self.collection_datetime

    def get_id(self) -> str:  # return test_id
        return self.test_id

    def get_name(self) -> str:  # return test_name
        return self.test_name

    def get_code(self) -> str:  # return code from SCT array
        return self.vendorSpecimenDescription[0]

    def get_unit(self) -> str:  # return LOINC for test
        return self.test_unit

    def print_all(self):
        print(f'Collection time: {self.collection_datetime}')
        print(f'Collection Site: {self.specimen_source}')
        # print(f'Test ID: {self.get_ID()}')
        print(f'Test Name: {self.get_name()}')
        print(f'Test Site: {self.specimen_source}')
        print(f'Test Code: {self.get_code()}')


class Order:
    def __init__(self, metadata: dict, patient: Patient, specimen: Specimen,
                 facility: Facility = None, provider: Provider = None,
                 result: Result = None):
        self.metadata = metadata
        self.patient = patient
        self.specimen = specimen
        self.facility = facility
        self.provider = provider
        self.result = result

        self.device: Device
        self.device = None

        self.line_item_names = []


class Device:
    def __init__(self, test_performed,
                 test_performed_long,
                 test_ordered,
                 test_ordered_long,
                 test_kit_id,
                 test_kit_id_type,
                 equipment_uid,
                 equipment_uid_type):
        self.test_performed = test_performed.replace('&', '\\T\\')
        self.test_performed_long = test_performed_long.replace('&', '\\T\\')
        self.test_ordered = test_ordered.replace('&', '\\T\\')
        self.test_ordered_long = test_ordered_long.replace('&', '\\T\\')
        self.test_kit_id = test_kit_id.replace('&', '\\T\\')
        self.test_kit_id_type = test_kit_id_type.replace('&', '\\T\\')
        self.equipment_uid = equipment_uid.replace('&', '\\T\\')
        self.equipment_uid_type = equipment_uid_type.replace('&', '\\T\\')

    def __str__(self) -> str:
        return f'Device<{self.test_performed} ({self.test_performed_long}) on {self.test_kit_id} {self.equipment_uid}>'
