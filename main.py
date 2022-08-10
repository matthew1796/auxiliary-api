
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

class TestMRNsRequest(BaseModel):
    num_mrns : int
    panelID : str

app = FastAPI()

date = datetime.now().strftime("%d-%m-%Y %H-%M-%S").replace(' ', '')
output_file = './fake_data/__output' + date + '.csv'


@app.get("/")
async def root():
    return {"message" : "Hello World"}

@app.post("/generate_test_mrns")
async def main(request : TestMRNsRequest):
    num_mrns = str(request.num_mrns)
    panelID = str(request.panelID)

    elis_agent = ELISAgent.ELISAgent()
    token = elis_agent.login();

    mockaroo_agent = MockarooAgent.MockarooAgent()
    mockaroo_agent.generate_fake_data(num_mrns)
    json_objs = mrn_generator.generate_json_objs(token, panelID)
    for post_body in json_objs:
        orderInfo = elis_agent.create_order(post_body)
        print(orderInfo)
        #open(output_file, 'w+').close();
        with open(output_file, 'a') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(orderInfo)
    return FileResponse(output_file)





def get_fake_data(num_mrns : int):
    response = request.get()
