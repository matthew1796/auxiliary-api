
from fastapi import FastAPI
from pydantic import BaseModel
from ELIS import ELISAgent
from Mockaroo import MockarooAgent
from fastapi.responses import StreamingResponse
from fastapi.responses import FileResponse
import io
import generate_fake_mrns as mrn_generator
import csv
from datetime import datetime
import pandas as pd

DATASHEET = './fake_data/fake_mrn_data.csv'
MAX_ELIS_ATTEMPS = 5

class TestMRNsRequest(BaseModel):
    num_mrns : int
    panelID : str

app = FastAPI()






@app.get("/")
async def root():
    return {"message" : "Hello World"}

# #Route for generating test mrns in ELIS
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
    token = elis_agent.login();

    #Create a mockaroo agent for hanling Mockaroo api calls
    mockaroo_agent = MockarooAgent()

    #Generate random fake patient data
    mockaroo_agent.generate_fake_data(num_mrns)

    #Loop through each row of patient data and generate a fake mrn and orderID for each patient
    fake_data = pd.read_csv(DATASHEET)
    for idx, row in fake_data.iterrows():

        #Loop that handles elis api calls and re-attempts them up to MAX_ELIS_ATTEMPS
        #if they fail
        success = False
        attempts = 0
        orderInfo = None
        while attempts <  MAX_ELIS_ATTEMPS:

            #Make the ELIS api call and store the response
            post_body = mrn_generator.generate_json_obj(token, panelID, row)
            orderInfo = elis_agent.create_order(post_body)

            #Token sometimes expires before generating all test mrns, if so,
            #get a new one and try again
            if orderInfo == None:
                attempts += 1
                print("Generating new token and trying again...")
                token = elis_agent.login()

            #If the api call was successfull, break out of the loop
            else:
                success = True
                break
        #If MAX_ELIS_ATTEMPS were made and was not successfull, then log this mrn as an error
        if not success:
            orderInfo = ('Error', 'Error')

        #write the orderInfo(mrn and orderID) to the next row in the output csv
        with open(output_file, 'a+') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(orderInfo)

    return FileResponse(output_file)
