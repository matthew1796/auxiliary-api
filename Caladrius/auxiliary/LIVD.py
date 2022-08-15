from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base
import pandas as pd

from caladrius.SQL import MySQLConnection
from caladrius.core import health

Base = declarative_base()


class LIVD_db(Base):
    __tablename__ = 'livd_mapping'
    id = Column(Integer, primary_key=True, unique=True)
    manufacturer = Column(String, nullable=False)
    model = Column(String, nullable=False)
    test_performed_loinc_code = Column(String, nullable=False)
    test_performed_long_name = Column(String, nullable=False)
    test_ordered_loinc_code = Column(String, nullable=False)
    test_ordered_long_name = Column(String, nullable=False)
    testkit_name_id = Column(String, nullable=False)
    testkit_name_id_type = Column(String, nullable=False)
    equipment_uid = Column(String, nullable=False)
    equipment_uid_type = Column(String, nullable=False)

    def __repr__(self):
        return "<LIVD(id='%s', manufacturer='%s', model='%s')>" \
               % (self.id, self.manufacturer, self.model)


mysql = MySQLConnection('resulting', 'production')
with mysql:
    query = mysql.session.query(LIVD_db)
    livd_mapping_tool = pd.read_sql(query.statement, query.session.bind)

livd_mapping_tool.set_index('id', inplace=True)


def get_livd_device(livd_id: int):
    row_current = livd_mapping_tool.loc[livd_id]
    test_performed_loinc_code = row_current['test_performed_loinc_code']
    test_performed_long_name = row_current['test_performed_long_name']
    test_ordered_loinc_code = row_current['test_ordered_loinc_code']
    test_ordered_long_name = row_current['test_ordered_long_name']
    testkit_name_id = row_current['testkit_name_id']
    testkit_name_id_type = row_current['testkit_name_id_type']
    equipment_uid = row_current['equipment_uid']
    equipment_uid_type = row_current['equipment_uid_type']
    return health.Device(test_performed_loinc_code, test_performed_long_name,
                         test_ordered_loinc_code, test_ordered_long_name,
                         testkit_name_id, testkit_name_id_type,
                         equipment_uid, equipment_uid_type)


if __name__ == '__main__':
    device = get_livd_device(819)
    print(device)
