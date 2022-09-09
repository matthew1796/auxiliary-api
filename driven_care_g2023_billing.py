import pandas as pd
import os
import re
from glob import glob
from Caladrius.billing  import compute_billing_range
from ELIS import ELISAgent
import json
from SQL import SQLAgent

output_home_dir = r'/mnt/c/Users/malonso/MedLab 2020/Ryan Farran - G2023 Billing Accession and DoC'





target_cpt_code = 'G2023'

def get_elis_specimen_id(id):
    print('getting specimenId')
    e_agent = ELISAgent()
    response = e_agent.get_order_details_by_id(id)

    return json.loads(response.content)["order"]["SpecimenID"]


def get_rejected_status(specimenID, sql_conn):
    cursor = sql_conn.cursor(dictionary=True)
    cursor.execute('SELECT * FROM rejections WHERE mrn = %s ', (specimenID, ))

    order = cursor.fetchone()

    if order:
        print('Found ', specimenID, 'in rejection log. Rejection date: ', order['rejection_date'])
        return order['rejection_date']
    else:
        print(specimenID, 'not found in rejection log')
        return None


def get_rejected_date(specimenID):
    return ""

def generate_G2023_only_reports(billing_ordinal : int):
    billing_archive_path = r'/mnt/p/billing_reports/archive/billing_' + str(billing_ordinal)
    print('Billing archive path:',billing_archive_path)
    medlab_billing_pattern = 'MedLab*with SpecimenID.csv'
    billing_reports = glob(os.path.join(billing_archive_path, medlab_billing_pattern))
    print('Found Billing Reports: ', billing_reports)
    for billing_report in billing_reports:
        generate_report(billing_ordinal, billing_report)



def generate_report(billing_ordinal, data_source):
    sql_agent = SQLAgent()
    sql_conn = sql_agent.create_connection('resulting')





    input_billing_report_df = pd.read_csv(data_source)
    input_billing_report_df = input_billing_report_df[input_billing_report_df['Billing CPT Code'].notna()]

    col_names = ["Accession Number", "SpecimenID", "Date of Collection", "Billing CPT Code", "Rejected Flag", "Rejected Date"]
    output_df = pd.DataFrame(columns=col_names)

    for idx, order in input_billing_report_df.iterrows():
        if target_cpt_code in order['Billing CPT Code']:
            output_df.loc[idx, 'Accession Number'] = order['Accession Number']
            orderID = order['Accession Number'][6:]
            specimenID =  order['SpecimenID']
            output_df.loc[idx, 'SpecimenID'] = specimenID
            output_df.loc[idx, 'Date of Collection'] = order['Date of Collection']
            output_df.loc[idx, 'Billing CPT Code'] = order['Billing CPT Code']
            rejection_date = get_rejected_status(specimenID, sql_conn)
            if rejection_date:
                output_df.loc[idx, 'Rejected Flag'] = 'y'
                output_df.loc[idx, 'Rejected Date'] = rejection_date



    billing_date_range = compute_billing_range(billing_ordinal)
    billing_date_range = [date_obj.strftime('%m-%d-%Y') for date_obj in billing_date_range]

    billing_date_str = ' ( ' + billing_date_range[0] +'--' + billing_date_range[1] + ' )'

    output_filename = 'G2023 Only -- ' + os.path.basename(data_source)
    output_dir = 'G2023 Only -- billing_' + str(billing_ordinal) + billing_date_str

    output_dir_path = os.path.join(output_home_dir, output_dir)

    if os.path.exists(output_dir_path) == False:
        os.mkdir(output_dir_path)

    output_path = os.path.join(output_dir_path, output_filename)

    output_df.to_csv(output_path, index=False)

    sql_conn.close()


if __name__ == '__main__':
    sql_agent = SQLAgent()
    conn = sql_agent.create_connection('resulting')
    get_rejected_status('02-1268266', conn)

    print('hello')
