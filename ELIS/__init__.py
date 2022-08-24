import json
import requests

credentials_path = './creds/ELIS_API_creds.json'

STAGE = 'test'

class ELISAgent ():

    def __init__(self):
        f = open(credentials_path)
        self.creds = json.load(f)[STAGE]
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
        api_endpoint = self.creds['base'] + self.create_order_path

        try:
            response = requests.post(api_endpoint, json=post_body)
            #response = requests.post("https://google.com", json=post_body)
            response.raise_for_status()
        except requests.HTTPError as e:
            print('Error:', e)
            return None
        except requests.exceptions.RequestException as e:
            print("Connection refused: ", e)
            return None
        except Exception as e:
            print('Internal server error', e)
            return None


        print('ELIS response: ', response.text)
        try:
            specimenId = post_body['specimenId']
            orderID = json.loads(response.text)['oid']
            orderID = 'E'+ orderID[6:]
            return specimenId, orderID
        except:
            return 'Error', {'errorMessage' : response.text['errorMessage']}
