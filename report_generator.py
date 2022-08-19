from SQL import SQLAgent
from Sparrow import SparrowAgent
from MongoDB import MongoAgent
import pandas as pd
from datetime import datetime, timedelta, timezone
import Caladrius.cc_archive.cc_db as caladrius
import json
import pymongo
import os
from pytz import timezone
import pytz

report_path = 'reports/oregon_late_expedited_orders(ALL).csv'



#---------------------------------------Functions---------------------------------------#

def get_mongo_orders(db, collection, query, projection=None):
    mongo_agent = MongoAgent()
    mongo_client = mongo_agent.create_connection()
    cc_orders = mongo_client[db][collection]
    mongo_cursor = cc_orders.find(query, projection)
    df = pd.DataFrame(list(mongo_cursor))
    mongo_agent.close()
    return df


def get_sql_orders(db, query):
    sql_agent = SQLAgent()
    connection = sql_agent.create_connection(db)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    df = pd.DataFrame(cursor)
    df = df.drop_duplicates(subset='MRN')
    sql_agent.close()
    return df


def get_collection_times(df):
     for index, row in df.iterrows():
         retrieved_order = caladrius.get_cc_order(row['MRN'])
         collection_time = retrieved_order.__dict__['specimen'].__dict__['collection_datetime']
         df.loc[index, 'Date of Collection'] = collection_time.astimezone(timezone('US/Pacific'))

def get_legacy_result_times(sql_db, df):
    sql_agent = SQLAgent()
    connection = sql_agent.create_connection(sql_db)
    for idx, row in df.iterrows():
        cursor = connection.cursor(dictionary=True,buffered=True)
        cursor.execute('SELECT * FROM resulting.legacy_results WHERE mrn = %s', (row['MRN'],))
        order = cursor.fetchone()
        result_timestamp = order['result_timestamp']
        result_timestamp = pytz.utc.localize(result_timestamp)
        df.loc[idx, 'Date of Result'] = result_timestamp.astimezone(timezone('US/Pacific'))
    sql_agent.close()

def contains_sunday(start : datetime, end: datetime):
    num_weeks, remainder = divmod( (end-start).days, 7)
    if ( 6 - start.weekday() ) % 7 < remainder:
       return num_weeks + 1
    else:
       return num_weeks

def time_until_end_of_day(dt=None):
    if dt is None:
        dt = datetime.datetime.now()
    return ((24 - dt.hour - 1) * 60 * 60) + ((60 - dt.minute - 1) * 60) + (60 - dt.second)

def is_late(start : datetime, end : datetime):

    start = start.replace(tzinfo=None)
    end = end.replace(tzinfo=None)


    deadline = start + timedelta(days=2)

    if contains_sunday(start, end):
        deadline += timedelta(days=1) #sundays dont count


    deadline += timedelta(seconds=time_until_end_of_day(deadline)) # have until midnight of the second day



    if end >= deadline:
        return True
    else:
        return False


def record_mongo_orders_by_price(price: int):
    mongo_query = {
        "raw_body.billing.state": "OR",
        "raw_body.line_items" : {
            "$elemMatch" : {
                "price" : price
            }
        }
    }

    mongo_projection = {
        "raw_body.line_items" : {
            "$elemMatch" : {
                "price" : price
            }
        }
    }

    oregon_exp_orders = get_mongo_orders("covidclinic", "orders", mongo_query)

    print("orders found by price: ", len(oregon_exp_orders.index))

def record_mongo_orders():
    test_names = ['Expedited RT-PCR COVID-19 Test - 1-2 Day Result', '*COVID-19 Test - MedLab2020 Expedited PCR Test 1-2 Day Results']

    for test in test_names:


        mongo_query = {
            "raw_body.billing.state": "OR",
            "raw_body.line_items" : {
                "$elemMatch" : {
                    "name" : test
                }
            }
        }

        oregon_exp_orders = get_mongo_orders("covidclinic", "orders", mongo_query)

        col_names = ["MRN", "Patient First Name", "Patient Last Name", "Test", "Date of Collection", "Date of Result"]
        report_df = pd.DataFrame(columns=col_names)

        for idx, row in oregon_exp_orders.iterrows():
            report_df.loc[idx, 'MRN'] = row['mrn']
            report_df.loc[idx, 'Patient First Name'] = row['raw_body']['billing']['first_name']
            report_df.loc[idx, 'Patient Last Name'] = row['raw_body']['billing']['last_name']
            report_df.loc[idx, 'Test'] = test

        get_collection_times(report_df)
        get_legacy_result_times('resulting', report_df)


        for idx, order in report_df.iterrows():
                if is_late(order['Date of Collection'], order['Date of Result']) == False:
                    report_df.drop(idx, inplace=True)

        report_df.to_csv(report_path, mode='a', header=not os.path.exists(report_path))

def record_sql_orders():
    oregon_exp_orders = get_sql_orders('_sparrow_', 'CALL get_oregon_expedited_orders()')
    col_names = ["MRN", "Patient First Name", "Patient Last Name", "Test", "Date of Collection", "Date of Result"]
    report_df = pd.DataFrame(columns=col_names)

    for idx, row in oregon_exp_orders.iterrows():
        report_df.loc[idx, 'MRN'] = row['MRN']
        report_df.loc[idx, 'Patient First Name'] = row['First Name']
        report_df.loc[idx, 'Patient Last Name'] = row['Last Name']
        report_df.loc[idx, 'Test'] = row['Test Type']
        result_timestamp = row['Date of Result']
        result_timestamp = pytz.utc.localize(result_timestamp)
        report_df.loc[idx, 'Date of Result'] = result_timestamp.astimezone(timezone('US/Pacific'))


    get_collection_times(report_df)



    for idx, order in report_df.iterrows():
            if is_late(order['Date of Collection'], order['Date of Result']) == False:
                report_df.drop(idx, inplace=True)

    report_df.to_csv(report_path, mode='a', header=not os.path.exists(report_path))


#---------------------------------------Main---------------------------------------#
if __name__ == "__main__":
    # record_mongo_orders()
    # record_sql_orders()
    record_mongo_orders_by_price(150)































#--------------------------------------------------------------
