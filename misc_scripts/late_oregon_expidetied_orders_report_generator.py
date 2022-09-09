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
import re

report_path = 'reports/expedited_orders(prod).csv'



#---------------------------------------Functions---------------------------------------#

def get_mongo_orders(mongo_client, db, collection, query, projection=None):

    cc_orders = mongo_client[db][collection]
    mongo_cursor = cc_orders.find(query, projection)
    #mongo_agent.close()
    return mongo_cursor


def get_sql_orders(db, query):
    sql_agent = SQLAgent()
    connection = sql_agent.create_connection(db)
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    df = pd.DataFrame(cursor)
    df = df.drop_duplicates(subset='MRN')
    cursor.close()
    sql_agent.close()
    return df


def get_collection_times(df):
     for index, row in df.iterrows():
         retrieved_order = caladrius.get_cc_order(row['MRN'])
         try:
             collection_time = retrieved_order.__dict__['specimen'].__dict__['collection_datetime']
             df.loc[index, 'Date of Collection'] = collection_time.astimezone(timezone('US/Pacific'))
         except:
            print('failed to get collection time for ', row['MRN'])



def get_result_time(prod_connection, legacy_connection,  mrn, stage) -> datetime:
    print('Fetching result time...')

    if stage == 'prodution':
        result_timestamp = get_prod_result_time(prod_connection, mrn)
        if result_timestamp is not None:
            return result_timestamp
        else:
            print('Result Timestamp for ' + mrn + ' not found in prod. Trying legacy results...')
            result_timestamp = get_legacy_result_time(legacy_connection, mrn)
            if result_timestamp is not None:
                print('Found result_timestamp for ' + mrn + 'in legacy results.')
                return result_timestamp
            else:
                print('No result timestamp found for ' + mrn)
                return None
    else:
        result_timestamp = get_legacy_result_time(legacy_connection, mrn)
        if result_timestamp is not None:
            return result_timestamp
        else:
            print('Result Timestamp for ' + mrn + ' not found in legacy results. Trying prod results...')
            result_timestamp = get_prod_result_time(prod_connection, mrn)
            if result_timestamp is not None:
                print('Found result_timestamp for ' + mrn + 'in prod results.')
                return result_timestamp
            else:
                print('No result timestamp found for ' + mrn)
                return None


def get_prod_result_time(connection,  mrn) -> datetime:

    try:
        cursor = connection.cursor(dictionary=True,buffered=True)
    except Exception as e:
        print(e)
        connection.reconnect()
        cursor = connection.cursor(dictionary=True,buffered=True)

    query_string = 'SELECT `rx`.`mrn` as `MRN` , \
                    `rn`.`result_timestamp` as `Date of Result` \
                     FROM resulting.results `rx` \
                     LEFT JOIN resulting.runs `rn` ON `rx`.`run_idx` = `rn`.`idx`\
                     WHERE `rx`.`mrn` = %s'

    cursor.execute( query_string, (mrn,))
    order = cursor.fetchone()
    cursor.close()
    if order is not None:
        result_timestamp = order['Date of Result']
        result_timestamp = pytz.utc.localize(result_timestamp)

        return result_timestamp.astimezone(timezone('US/Pacific'))
    else:
        return None

def get_legacy_result_time(connection, mrn) -> datetime:
    try:
        cursor = connection.cursor(dictionary=True,buffered=True)
    except Exception as e:
        print(e)
        connection.reconnect()
        cursor = connection.cursor(dictionary=True,buffered=True)
    cursor.execute('SELECT * from legacy_results WHERE mrn = %s', (mrn,))
    order = cursor.fetchone()
    cursor.close()
    if order is not None:
        result_timestamp = order['result_timestamp']
        result_timestamp = pytz.utc.localize(result_timestamp)
        return result_timestamp.astimezone(timezone('US/Pacific'))
    else:
        return None

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

def is_late(start : datetime, end : datetime) :

    print(start, end)

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

def record_by_json():
    col_names = ["MRN", "Patient First Name", "Patient Last Name", "Test", "Date of Collection", "Date of Result"]
    report_df = pd.DataFrame(columns=col_names)

    sql_agent = SQLAgent()
    prod_connection = sql_agent.create_connection('_sparrow_')
    legacy_connection = sql_agent.create_connection('resulting')

    with open('reports/oregon_expedited_orders(using meta data).json') as oregon_exp_orders_file:
        oregon_exp_orders = json.load(oregon_exp_orders_file)
        idx = 0
        for row in oregon_exp_orders:
            print(idx)
            report_df.loc[idx, 'MRN'] = row['mrn']
            report_df.loc[idx, 'Patient First Name'] = row['raw_body']['billing']['first_name']
            report_df.loc[idx, 'Patient Last Name'] = row['raw_body']['billing']['last_name']
            for test in row['raw_body']['line_items']:
                if 'Expedited' in test['name']:
                    report_df.loc[idx, 'Test'] = test['name']
                    break
            report_df.loc[idx, 'Date of Result'] = get_result_time(prod_connection, legacy_connection, row['mrn'], row['stage'])
            idx += 1


    get_collection_times(report_df)

    for idx, order in report_df.iterrows():
            try:
                if is_late(order['Date of Collection'], order['Date of Result']) == False:
                    report_df.drop(idx, inplace=True)
            except Exception as e:
                print(e)
                print('Failed on ', order['MRN'])


    prod_connection.close()
    legacy_connection.close()
    report_df.to_csv(report_path, mode='a', header=not os.path.exists(report_path))



def record_mongo_orders_prod(test, prod_connection, legacy_connection, mongo_client):

        mongo_query = {'stage': 'production', 'raw_body.patient_information.patients_state' : 'OR', 'raw_body.line_items': {'$elemMatch': { 'name': test}}}

        oregon_exp_orders = get_mongo_orders(mongo_client, "covidclinic", "orders", mongo_query)

        col_names = ["MRN", "Patient First Name", "Patient Last Name", "Test", "Date of Collection", "Date of Result"]
        report_df = pd.DataFrame(columns=col_names)

        idx = 0
        for row in oregon_exp_orders:
            report_df.loc[idx, 'MRN'] = row['mrn']
            report_df.loc[idx, 'Patient First Name'] = row['raw_body']['patient_information']['patient_first_name']
            report_df.loc[idx, 'Patient Last Name'] = row['raw_body']['patient_information']['patient_last_name']
            report_df.loc[idx, 'Test'] = test
            report_df.loc[idx, 'Date of Result'] = get_result_time(prod_connection, legacy_connection, row['mrn'], row['stage'])
            print(report_df.loc[[idx]])
            idx += 1

        get_collection_times(report_df)



        for idx, order in report_df.iterrows():
            try:
                if is_late(order['Date of Collection'], order['Date of Result']) == False:
                    report_df.drop(idx, inplace=True)
            except Exception as e:
                print(e)
                print('Failed on ', order['MRN'])




        print(test, "Total: ", len(report_df.index) )
        report_df.to_csv(report_path, mode='a', header=not os.path.exists(report_path))





def record_mongo_orders_by_price(price):
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

    col_names = ["MRN", "Patient First Name", "Patient Last Name", "Test", "Date of Collection", "Date of Result"]
    report_df = pd.DataFrame(columns=col_names)

    sparrow_sql_agent = SQLAgent()
    resulting_sql_agent = SQLAgent()

    sparrow_conn = sparrow_sql_agent.create_connection('_sparrow_')
    resulting_conn = resulting_sql_agent.create_connection('resulting')

    for idx, row in oregon_exp_orders.iterrows():
        report_df.loc[idx, 'MRN'] = row['mrn']
        report_df.loc[idx, 'Patient First Name'] = row['raw_body']['billing']['first_name']
        report_df.loc[idx, 'Patient Last Name'] = row['raw_body']['billing']['last_name']
        report_df.loc[idx, 'Date of Result'] = get_result_time(sparrow_conn, resulting_conn, row['mrn'], row['stage'])
        for test in row['raw_body']['line_items']:
            if test['price'] == 150:
                report_df.loc[idx, 'Test'] = test['name']
                break

    get_collection_times(report_df)

    for idx, order in report_df.iterrows():
            if is_late(order['Date of Collection'], order['Date of Result']) == False:
                report_df.drop(idx, inplace=True)

    report_df.to_csv(report_path, mode='a', header=not os.path.exists(report_path))
    sparrow_sql_agent.close()
    resulting_sql_agent.close()



def record_mongo_orders_archive(test, prod_connection, legacy_connection, mongo_client):


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

        idx = 0
        for row in oregon_exp_orders:
            report_df.loc[idx, 'MRN'] = row['mrn']
            report_df.loc[idx, 'Patient First Name'] = row['raw_body']['billing']['first_name']
            report_df.loc[idx, 'Patient Last Name'] = row['raw_body']['billing']['last_name']
            report_df.loc[idx, 'Test'] = test
            report_df.loc[idx, 'Date of Result'] = get_result_time(prod_connection, legacy_connection, row['mrn'], row['stage'])
            print(idx, report_df.loc[[i]])
            idx += 1

        get_collection_times(report_df)



        for idx, order in report_df.iterrows():
            try:
                if is_late(order['Date of Collection'], order['Date of Result']) == False:
                    report_df.drop(idx, inplace=True)
            except Exception as e:
                print(e)
                print('Failed on ', order['MRN'])




        print(test, "Total: ", len(report_df.index) )
        report_df.to_csv(report_path, mode='a', header=not os.path.exists(report_path))





def record_all_mongo_orders(test_names : list):
    sql_agent = SQLAgent()
    prod_connection = sql_agent.create_connection('_sparrow_')
    legacy_connection = sql_agent.create_connection('resulting')

    mongo_agent = MongoAgent()
    mongo_client = mongo_agent.create_connection()

    for test in test_names:
        record_mongo_orders_archive(test, prod_connection, legacy_connection, mongo_client)
        record_mongo_orders_prod(test, prod_connection, legacy_connection, mongo_client)



    prod_connection.close()
    legacy_connection.close()
    mongo_client.close()



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

    print('SQL Total: ', len(report_df.index))

    report_df.to_csv(report_path, mode='a', header=not os.path.exists(report_path))


#---------------------------------------Main---------------------------------------#
if __name__ == "__main__":
    test_names = ['Expedited RT-PCR COVID-19 Test - 1-2 Day Result', '*COVID-19 Test - MedLab2020 Expedited PCR Test 1-2 Day Results',
                  'ExpeditedRT-PCRCOVID-19Test-1-2DayResult', '*Expedited RT-PCR COVID-19 Test - 1-2 Day Result', 'Expedited RT-PCR COVID-19 Test',
                  '$0 No Cost to Patient 2-3 Day Expedited RT-PCR Test', '*A. Expedited RT-PCR Test', '*A. Expedited RT-PCR Test (Backup)',
                  '*A. Expedited RT-PCR Test (No Cost To Patient)' ]

    record_all_mongo_orders(test_names)































#--------------------------------------------------------------
