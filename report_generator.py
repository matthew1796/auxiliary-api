from SQL import SQLAgent
from Sparrow import SparrowAgent
from MongoDB import MongoAgent
import pandas as pd
from datetime import datetime, timedelta
import Caladrius.cc_archive.cc_db as caladrius
import json
import pymongo

report_path = 'reports/oregon_expedited_orders_with_cxt.csv'

def get_orders():
    sql_agent = SQLAgent()
    connection = sql_agent.create_connection('_sparrow_')
    cursor = connection.cursor(dictionary=True)
    cursor.execute('CALL get_oregon_expedited_orders()')
    df = pd.DataFrame(cursor)
    df = df.drop_duplicates(subset='MRN')
    return df


def get_collection_times(df):
     for index, row in df.iterrows():
         retrieved_order = caladrius.get_cc_order(row['MRN'])
         collection_time = retrieved_order.__dict__['specimen'].__dict__['collection_datetime']
         df.loc[index, 'Date of Collection'] = collection_time
         return df

def contains_sunday(start : datetime, end: datetime):
    num_weeks, remainder = divmod( (end-start).days, 7)
    if ( 6 - start.weekday() ) % 7 < remainder:
       return num_weeks + 1
    else:
       return num_weeks

def is_late(start : datetime, end : datetime):
    elapsed_time = end - start
    time_limit = 3600*48 # 48 hours
    if contains_sunday:
        time_limit += 3600*24

    if elapsed_time.total_seconds() >= time_limit:
        return True
    else:
        return False

mongo_agent = MongoAgent()
mongo_client = mongo_agent.create_connection()
cc_orders = mongo_client["covidclinic"]["orders"]

cursor = cc_orders.find()



df = pd.DataFrame(list(cursor))



for index, order in df.iterrows():
    contains_expedited_test = False
    for test in order["raw_body"]["line_items"]:
        if test['name'] == "*COVID-19 Test - MedLab2020 Expedited PCR Test 1-2 Day Results":
            contains_expedited_test = True
            break
    if contains_expedited_test == False:
        df.drop(index, inplace=True)
df.to_csv(report_path)

mongo_agent.close()

# OR_exp_df = pd.read_csv(report_path)
#
# for idx, row in OR_exp_df.iterrows():
#     cx_date = row['Date of Collection'][0:19]
#     cx_date = datetime.strptime(cx_date, "%m/%d/%Y %H:%M")
#     rx_date = row['Date of Result']
#     rx_date = datetime.strptime(rx_date, "%m/%d/%Y %H:%M")
#     if rx_date < cx_date:
#         OR_exp_df.drop(idx, inplace=True)
#     if is_late(cx_date, rx_date) == False:
#         OR_exp_df.drop(idx, inplace=True)
#
#
#
#
#
#
# OR_exp_df.to_csv('reports/oregon_expedited_orders_with_cxt.csv')
