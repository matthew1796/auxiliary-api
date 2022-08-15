# just some helper functions
from datetime import datetime

from woocommerce import API as wcapi

from Caladrius.core.pii import Name


def noComma(word: str):
    return word.replace(',', '')


def xstr(s):
    return '' if s is None else str(s)


def xdt(dt: datetime, fmt: str, default: str = ''):
    if not dt:
        return default
    else:
        return dt.strftime(fmt)


def xname(name: Name, default: str = ''):
    if not name:
        return default
    else:
        return name.get_fullname()


api_dict = {
    '00': wcapi(
        url="https://wcapi.covidclinic.org/",
        consumer_key='ck_9646437b958df170a1c09bcb6291d1b26a76bbcf',
        consumer_secret='cs_97f19bb40df2866910ceb5fa9ace175d96fe76b0',
        version="wc/v3",
        timeout=10,
        query_string_auth=True,
        wp_api=True
    ),
    '02': wcapi(
        url="https://mobile.covidclinic.org/",
        consumer_key="ck_69451242aadc88907a0cb002a75ce52e2ae0af1a",
        consumer_secret="cs_33e34d669218926367a99f1aa021c983c9465d9b",
        version="wc/v3",
        timeout=10,
        query_string_auth=True,
        wp_api=True,
    )
}


def string_compare(x: str, y: str):
    if x == y:
        return '1'
    else:
        return '0'


tx_header = [
    'Reporting_Facility_Name', 'CLIA_Number', 'Performing_Organization_Name', 'Performing_Organization_Address',
    'Performing_Organization_City', 'Performing_Organization_Zip', 'Performing_Organization_State', 'Device_Identifier',
    'Ordered_Test_Name', 'LOINC_Code', 'LOINC_Text', 'Result', 'Result_Units', 'Reference_Range', 'Date_Test_Performed',
    'Test_Result_Date', 'Pt_Fname', 'Pt_Middle_Initial', 'Pt_Lname', 'Date_of_Birth', 'Patient Age', 'Sex', 'Pt_Race',
    'Pt_Ethnicity', 'Pt_Phone', 'Pt_Str', 'Pt_City', 'Pt_ST', 'Pt_Zip', 'Pt_County', 'Accession_Number',
    'Ordering_Facility',
    'Ordering_Facility_Address', 'Ordering_Facility_City', 'Ordering_Facility_State', 'Ordering_Facility_Zip',
    'Ordering_Provider_Last_Name',
    'Ordering_Provider_First_Name', 'Ordering_Provider_NPI', 'Ordering_Provider_Street_Address',
    'Ordering_Provider_City',
    'Ordering_Provider_State', 'Ordering_Provider_Zip', 'Ordering_Provider_Phone', 'Specimen_ID', 'Specimen_Type',
    'Date_Test_Ordered', 'Date_Specimen_Collected', 'Medical_Record_Number', 'Employed_in_healthcare',
    'Symptomatic_per_cdc',
    'Date_of_Symptom_Onset', 'Hospitalized', 'ICU', 'Resident_in_Congregate_Care', 'Pregnant'
]

or_header = [
    'Sending Application', 'Facility Name', 'Facility CLIA', 'Facility Street Address', 'Facility City',
    'Facility State', 'Facility Zip', 'Facility Phone', 'Date/Time of Message', 'Patient Identifier',
    'Patient First Name', 'Patient Last Name', 'Patient Date Of Birth', 'Patient Sex', 'Race', 'Ethnicity',
    'Language', 'Patient Street Address', 'Patient City', 'Patient State', 'Patient Zip',
    'Patient County', 'Patient Phone Number', 'OK to Contact Patient', 'Insurance',
    'Expedited Partner Therapy Received', 'Provider First Name', 'Provider Last Name', 'Provider Phone Number',
    'Specimen ID',
    'Collection Date', 'Specimen Type', 'Specimen Site', 'Test Name', 'Result', 'Notes',
    'First Test', 'Employed In Health Care', 'Symptomatic As defined by CDC', 'Symptom Onset',
    'Hospitalized', 'ICU', 'Resident In Congregate Care Setting', 'Pregnant'
]

fl_header = [
    'RecordID', 'FacilityID', 'CLIAID', 'AccessionNumber', 'ClientID', 'LastName', 'FirstName',
    'MiddleName', 'DOB', 'SSN', 'StreetAddress', 'City', 'State', 'Zip', 'County', 'Gender', 'PhoneNumber',
    'Ethnicity', 'RaceWhite', 'RaceBlack', 'RaceAmericanIndianAlaskanNative', 'RaceAsian',
    'RaceNativeHawaiianOrOtherPacificIslander',
    'RaceOther', 'RaceUnknown', 'RaceNoResponse', 'ProviderName', 'NPI', 'Pregnant', 'SchoolAssociation',
    'SchoolName', 'SpecimenCollectionSite', 'SpecimenSNOMED', 'SpecimenCollectedDate', 'SpecimenReportedDate',
    'RapidTest', 'Type', 'ModelOrComponent', 'LOINC', 'TestName', 'SNOMED', 'Result'
]

nv_header = [
    'SendingFacility', 'Patient_ID', 'MRN', 'PtLastName', 'PtFirstName',
    'PtMI', 'DateOfBirth', 'Sex', 'Race', 'Ethnicity', 'PatientStreet',
    'PatientStreet2', 'PatientCity', 'PatientState', 'PatientZipcode',
    'PatientPhoneNumber', 'AccessionNumber', 'OrderedTest_ID',
    'OrderedTest_name', 'SpecimenCollectionDate', 'SpecimenSite',
    'OrderingProviderLastName', 'OrderingProviderFirstName',
    'OrderingFacilityName', 'OrderingFacilityAddress', 'OrderingFacilityCity',
    'OrderingFacilityState', 'OrderingFacilityZIP', 'OrderingFacilityPhone',
    'ResultedTestID', 'ResultedTestName', 'TestResult', 'TestDate', 'PerformingLab', 'Notes'
]

va_header = [
    'Sending Facility Name', 'Sending Facility CLIA', 'Message Control ID',
    'PatientID', 'SSN', 'Last Name', 'First Name', 'Middle Initial', 'Street Address',
    'Street Address 2', 'City', 'County FIPS Code', 'State', 'Zip', 'Patient Phone',
    'Race', 'Ethnic Group', 'DOB', 'Sex', 'Message Date Time', 'Specimen ID',
    'Specimen Type Description', 'Specimen Source Site Text', 'Result Unit ID',
    'Provider ID', 'Provider Last Name', 'Provider First Name',
    'Ordering Provider Addr 1', 'Ordering Provider Addr 2', 'Ordering Provider City', 'Ordering Provider State',
    'Ordering Provider Zip', 'Ordering Provider County FIPS code', 'Ordering Provider Phone',
    'Ordering Facility Name', 'Ordering Facility Address 1', 'Ordering Facility Address 2',
    'Ordering Facility City', 'Ordering Facility State', 'Ordering Facility Zip', 'Ordering Facility County FIPS Code',
    'Ordering Facility Phone', 'Observation Date Time', 'Result Status', 'Specimen Received Date',
    'Order Code', 'Order Code Text Description', 'Order Code Naming system', 'Result Value Type',
    'Result Test code', 'Result Test Text Description', 'Result Test Naming system', 'Observation Value',
    'Observation Value/Result Text',
    'Observation Value/Result Naming system', 'Test Result Status', 'Performing Lab ID/Producer ID',
    'Performing Lab ID/Producer Text',
    'Performing Lab ID/Producer Naming System', 'Date Reported', 'Performing Lab Street Address line 1',
    'Performing Lab Street Address line 2', 'Performing Lab City', 'Performing Lab State', 'Performing Lab Zip',
    'Performing Lab County FIPS Code', 'Specimen Type Identifier', 'Speciment Type Naming System', 'Date test ordered',
    'EUA based test kit identification', 'Model name based test kit identification',
    'Device identifier based test kit identification',
    'Model name based instrument identification', 'Device identifier based instrument identification',
    'Instance based test kit identification',
    'Instance based instrument identification', 'Patient\'s Age Value', 'Patient\'s Age Units',
    'First Test', 'Employed in healthcare', 'Symptomatic', 'Date of Symptom Onset', 'Hospitalized', 'ICU',
    'Congregate care setting', 'Pregnant'
]
