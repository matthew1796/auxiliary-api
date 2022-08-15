from datetime import datetime

from sqlalchemy import Column, String, Date, DateTime, Integer
from sqlalchemy.orm import declarative_base

from Caladrius.core.entities import Patient as PatientObject

Base = declarative_base()


class Patient(Base):
    __tablename__ = 'patients'
    mrn = Column(String, primary_key=True)
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    dob = Column(Date)
    sex = Column(String)
    race = Column(String)
    ethnicity = Column(String)
    street_1 = Column(String)
    street_2 = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    email = Column(String)
    phone = Column(String)
    language = Column(String)
    third_party_emails = Column(String)
    dln = Column(String)
    ssn = Column(String)
    creation_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Patient(mrn='%s', last_name='%s')>" % (self.mrn, self.last_name)


def serialize_patient(pt: PatientObject):
    tpe = pt.contact.third_party_emails
    if not isinstance(tpe, list):
        tpe = []
    return Patient(
        mrn=pt.mrn,
        first_name=pt.name.get_fname(),
        middle_name=pt.name.get_mname(),
        last_name=pt.name.get_lname(),
        dob=pt.DOB.date(),
        sex=pt.contact.sex.abbr,
        race=pt.contact.race.code,
        ethnicity=pt.contact.ethnicity.value,
        street_1=pt.address.street_1,
        street_2=pt.address.street_2,
        city=pt.address.city,
        state=pt.address.state,
        zip=pt.address.zip,
        email=pt.contact.email,
        phone=pt.contact.phone,
        language=pt.contact.language,
        third_party_emails=','.join(tpe),
        dln=pt.billing.drivers_license_number,
        ssn=pt.billing.social_security_number,
        creation_timestamp=datetime.utcnow()
    )


class Billing(Base):
    __tablename__ = 'billing'
    mrn = Column(String, primary_key=True)
    ordinality = Column(Integer)
    insurance_name = Column(String)
    payer_first_name = Column(String)
    payer_last_name = Column(String)
    payer_dob = Column(Date)
    insurance_id = Column(String)
    insurance_group = Column(String)
    subscriber_first_name = Column(String)
    subscriber_last_name = Column(String)
    subscriber_dob = Column(Date)
    creation_timestamp = Column(DateTime)


def serialize_billing(pt: PatientObject, ordinality=1):
    billing = pt.billing
    payer_name = billing.payer_name
    subscriber_name = billing.subscriber_name
    subscriber_dob = billing.subscriber_dob
    return Billing(
        mrn=pt.mrn,
        ordinality=ordinality,
        insurance_name=billing.insurance_name,
        payer_first_name=payer_name.get_fname() if payer_name else None,
        payer_last_name=payer_name.get_lname() if payer_name else None,
        payer_dob=None,
        insurance_id=billing.insurance_id,
        insurance_group=billing.insurance_group,
        subscriber_first_name=subscriber_name.get_fname() if subscriber_name else None,
        subscriber_last_name=subscriber_name.get_lname() if subscriber_name else None,
        subscriber_dob=subscriber_dob.date() if subscriber_dob else None,
        creation_timestamp=datetime.utcnow()
    )


class LineItem(Base):
    __tablename__ = 'orders'
    mrn = Column(String, primary_key=True)
    line_item_name = Column(String, primary_key=True)
    creation_timestamp = Column(DateTime)

    def __repr__(self):
        return "<LineItem(mrn='%s', line_item_name='%s')>" % (self.mrn, self.line_item_name)


class Symptom(Base):
    __tablename__ = 'symptoms'
    mrn = Column(String, primary_key=True)
    symptom = Column(String, primary_key=True)
    creation_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Symptom(mrn='%s', symptom='%s')>" % (self.mrn, self.symptom)


if __name__ == '__main__':
    import pandas as pd
    from caladrius.DocumentDB import MongoClient

    from primary_automation.billing.audit import result_tables

    results_df = result_tables.df
    resulted_mrns = set(results_df['mrn'])
    with MongoClient('master') as db_client:
        orders_collection = db_client['covidclinic']['orders']
        print('Projecting all MRNs...')
        response = orders_collection.find(projection={'mrn': True})
        for x in response:
            mrn = x['mrn']
            if mrn in resulted_mrns:
                print(f'Got [{mrn}]...')
                resulted_mrns.remove(mrn)
            else:
                print(f'Unable to find [{mrn}] in resulted mrns...')
    pd.DataFrame({'mrn': list(resulted_mrns)}).to_csv('results_not_in_mongodb.csv', index=False)
    print()
#
#     remaining_mrns.drop_duplicates(subset=['mrn'], keep='first', inplace=True)
