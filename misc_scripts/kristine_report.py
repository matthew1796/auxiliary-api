import pandas as pd
import sys
import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from ELIS import ELISAgent

report_path = 'reports/OPTUM_billing.csv'
pdf_parent_dir = 'test_pdfs'
req_dir = 'requisition_forms'
res_dir = 'result_forms'


if __name__ == "__main__":
    report_df = pd.read_csv(report_path)

    elis_agent = ELISAgent()



    for idx, order in report_df.iterrows():
        pt_fn = order['Patient First Name']
        pt_ln = order['Patient Last Name']
        pt_fullname = str(pt_fn) + '_' + str(pt_ln)
        pt_dob = order['Patient DOB'].replace('/','-')
        collection_date = order['Date of Collection'].replace('/','-')
        orderID = order['Accession Number'].split('0', 1)[1]
        print(orderID)
        req_filename = '/' +  pt_fullname + '_' + collection_date +'req.pdf'
        res_filename = '/' +  pt_fullname + '_' + collection_date +'res.pdf'
        post_body = {
            "token": elis_agent.login(),
            "id": int(orderID)
        }
        patient_dir = pt_fullname + '_' + pt_dob
        patient_dir_path = os.path.join(pdf_parent_dir, patient_dir)
        if os.path.exists(patient_dir_path) == False:
            os.mkdir(patient_dir_path)

        req_dir_path = os.path.join(patient_dir_path, req_dir)
        if os.path.exists(req_dir_path) == False:
            os.mkdir(req_dir_path)
        res_dir_path = os.path.join(patient_dir_path, res_dir)
        if os.path.exists(res_dir_path) == False:
            os.mkdir(res_dir_path)

        elis_agent.get_req_pdf(post_body, req_dir_path + req_filename)
        elis_agent.get_res_pdf(post_body, res_dir_path + res_filename)
