import json
import requests

credentials_path = './creds/ELIS_API_creds.json'



class ELISAgent ():

    def __init__(self):
        f = open(credentials_path)
        self.creds = json.load(f)
        self.login_path = '/token';
        self.create_order_path = '/order/create'
        f.close();



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

        return json.loads(response.text)['token'];

    def create_order(self, post_body):
        print('in create order')
        api_endpoint = self.creds['base'] + self.create_order_path
        response = requests.post(api_endpoint, json=post_body)
        print(response.text)
        specimenId = post_body['specimenId']
        orderID = json.loads(response.text)['oid']

        # specimenId = 'XX-987656924'
        # orderID = 'DEFCV03027527'
        orderID = 'E'+ orderID[6:]
        return specimenId, orderID
