import json
import requests
import pandas as pd
import datetime
from io import StringIO
import time
from urllib.error import HTTPError
credentials_path = './creds/ELIS_API_creds.json'

STAGE = 'production'

class ELISAgent ():

    def __init__(self):
        f = open(credentials_path)
        self.token = None
        self.creds = json.load(f)[STAGE]
        self.login_path = '/token';
        self.create_order_path = '/order/create'
        self.get_extended_billing_path = '/billing/csv/extended'
        self.get_req_pdf_path = '/lab/order/req/pdf'
        self.get_res_pdf_path = '/lab/order/results/pdf'
        self.get_order_details_path = '/lab/order/details'

        f.close();

    def post(self, endpoint, payload):
        if self.token is None:
            self.login()

        response = None
        max_attempts = 2
        retry_wait = 0.25

        i = 0
        success = False
        while i <= max_attempts and not success:
            payload.update({'token': self.token})
            try:
                response = requests.post(endpoint, json=payload, headers={'Content-Type': 'application/json'})
                response.raise_for_status()
                try:
                    if response.json() and response.json().get('errorCode') == 403:
                        raise HTTPError('403 Error: Forbidden')
                except Exception as e:
                    if e.__class__.__name__ == 'JSONDecodeError':
                        pass
                    else:
                        raise e

                success = True
            except HTTPError:
                traceback.print_exc()
                print("Generating new token and trying again...")
                self.login()
            time.sleep(retry_wait)
            i += 1

        if response is None:
            raise ValueError('No ELIS response')

        return response


    def login(self):
        obj = {
            "apiKey" : self.creds['apiKey'],
            "apiSecret" : self.creds['apiSecret']
        }
        api_endpoint = self.creds['base'] + self.login_path
        try:
            response = requests.post(api_endpoint, json=obj)
        except requests.exceptions.RequestException as e :
            print(e)

        self.token = json.loads(response.text)['token'];

    def get_order_details_by_specID(self, specimenId):
        api_endpoint = self.creds['base'] + self.get_order_details_path

        payload = {
            "specimenId": specimenId
        }

        response = self.post(api_endpoint, payload)

        return response

    def get_order_details_by_id(self, id : int):
        api_endpoint = self.creds['base'] + self.get_order_details_path

        payload = {
            'id' : id
        }

        response = self.post(api_endpoint, payload)

        return response

    def get_extended_billing(self, start_date : datetime, end_date : datetime) -> pd.DataFrame:
        api_endpoint = self.creds['base'] + self.get_extended_billing_path


        payload = {
            'fromDate' : start_date.strftime('%Y-%m-%d'),
            'toDate' : end_date .strftime('%Y-%m-%d'),

        }


        response = self.post(api_endpoint, payload)

        return pd.read_csv(StringIO(response.content.decode('utf-8')), dtype=str)


    def create_order(self, payload):
        api_endpoint = self.creds['base'] + self.create_order_path

        response = self.post(api_endpoint, payload)


        print('ELIS response: ', response.text)
        try:
            specimenId = payload['specimenId']
            orderID = json.loads(response.text)['id']
            orderID = 'E' + orderID
            return specimenId, orderID
        except:
            return ('Error', {'errorMessage' : json.loads(response.text)['errorMessage']})

    def get_req_pdf(self, payload, storage_path):
        api_endpoint = self.creds['base'] + self.get_req_pdf_path

        response = self.post(api_endpoint, payload)

        with open(storage_path, 'wb') as f:
            f.write(response.content)

    def get_res_pdf(self, payload, storage_path):
        api_endpoint = self.creds['base'] + self.get_res_pdf_path

        response = self.post(api_endpoint, payload)

        with open(storage_path, 'wb') as f:
            f.write(response.content)
