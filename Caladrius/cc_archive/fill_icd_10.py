import traceback
from time import sleep

from sqlalchemy import Column, String, sql
from sqlalchemy.orm import declarative_base, sessionmaker

from caladrius.SQL import SQLAgent
import pandas as pd

from caladrius.auxiliary.icd_10 import icd_10_map
from cc_archive import cc_db

sql_agent = SQLAgent('production')

Base = declarative_base()


class ICD10(Base):
    __tablename__ = 'icd_10_codes'
    mrn = Column(String, primary_key=True, nullable=False)
    icd_10 = Column(String, primary_key=True, nullable=False)

    def __repr__(self):
        return "<ICD10(mrn='%s', icd_10='%s')>" \
               % (self.mrn, self.icd_10)


with sql_agent.get_connection('covid_clinic') as conn:
    df = pd.read_sql(sql.text('call covid_clinic.ax_results(2, NULL, 10000000, 0);'), conn)
    already_done = pd.read_sql(sql.text('SELECT * FROM covid_clinic.icd_10_codes'), conn)

    session = sessionmaker(bind=sql_agent.get_engine('covid_clinic'))()

    mrns_left = list(set(pd.unique(df.mrn)) - set(already_done.mrn))


    def divide_chunks(l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]


    for mrns in divide_chunks(mrns_left, 20):
        print(mrns)
        try:
            for mrn, order in cc_db.get_cc_order(mrns).items():
                if order.patient.symptoms.symptoms is None:
                    print(f'No symptoms for mrn=[{mrn}]')
                    continue

                icd_10_codes = [icd_10_map.get(symptom) for symptom in order.patient.symptoms.symptoms if
                                symptom in icd_10_map]
                for icd_10_code in icd_10_codes:
                    icd10 = ICD10(mrn=mrn, icd_10=icd_10_code)
                    print(icd10)
                    session.merge(icd10)

            session.commit()
        except Exception as e:
            traceback.print_exc()
            sleep(1)

print()
