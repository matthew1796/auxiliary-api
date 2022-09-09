
from fastapi import FastAPI
from pydantic import BaseModel
from ELIS import ELISAgent
from Mockaroo import MockarooAgent
import Caladrius.billing.automate_billing as automate_billing
from Caladrius.billing import compute_billing_range
from Caladrius.billing.render_elis_billing_report import generate_elis_report_with_specID

from fastapi.responses import StreamingResponse
from fastapi.responses import FileResponse
import io
import generate_fake_mrns as mrn_generator
import csv
from datetime import datetime
import pandas as pd
import json
import driven_care_g2023_billing as G2023_report_generator

DATASHEET = './fake_data/fake_mrn_data.csv'

#Classes to capture json requests
class BillingRequest(BaseModel):
    billing_ordinal : int

class TestMRNsRequest(BaseModel):
    num_mrns : int
    panelID : str

app = FastAPI()




@app.get("/")
async def root():
    return {"message" : "Hello World"}

@app.get('/test')
async def test():
    e_agent = ELISAgent()
    response = e_agent.get_order_details_by_id(2540170)

    return json.loads(response.content)


@app.post('/compute_billing_range')
async def get_billing_range(request : BillingRequest):
    billing_ordinal = request.billing_ordinal

    date_range = compute_billing_range(billing_ordinal)

    response = {
        'start_date' : date_range[0],
        'end_date' : date_range[1]
    }
    return response


@app.post("/generate_G2023_only_reports")
async def generate_G2023_only_reports(request: BillingRequest):

    billing_ordinal = request.billing_ordinal
    generate_elis_report_with_specID(billing_ordinal)
    G2023_report_generator.generate_G2023_only_reports(billing_ordinal)

@app.post("/generate_billing_reports")
async def generate_billing_reports(request: BillingRequest):
    billing_ordinal = request.billing_ordinal
    automate_billing.generate_billing_reports(billing_ordinal)

#Route for generating test mrns in ELIS
@app.post("/generate_test_mrns")
async def generate_test_mrns(request : TestMRNsRequest):

    #Generate a unique output file name
    date = datetime.now().strftime("%d_%m_%YT%H%M%S")
    output_file = './fake_data/test_mrns_' + date + '.csv'

    #Get the request post body
    num_mrns = str(request.num_mrns)
    panelID = str(request.panelID)

    #Create an elis agent for handling ELIS api calls
    elis_agent = ELISAgent()

    #Generate an ELIS token for making api calls
    elis_agent.login();

    #Create a mockaroo agent for handling Mockaroo api calls
    mockaroo_agent = MockarooAgent()

    #Generate random fake patient data
    mockaroo_agent.generate_fake_data(num_mrns)

    #Loop through each row of patient data and generate a fake mrn and orderID for each patient
    fake_data = pd.read_csv(DATASHEET)
    for idx, row in fake_data.iterrows():


        #Make the ELIS api call and store the response
        post_body = mrn_generator.generate_json_obj( panelID, row)
        orderInfo = elis_agent.create_order(post_body)



        if orderInfo[0] == 'Error':
            print('Error:', orderInfo[1])
            return orderInfo


        #write the orderInfo(mrn and orderID) to the next row in the output csv
        with open(output_file, 'a+') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(orderInfo)

    return FileResponse(output_file)
