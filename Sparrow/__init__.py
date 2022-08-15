import json
import requests

credentials_path = "./creds/sparrow_creds.json"
stage = "test"

class SparrowAgent():
    def __init__(self):
        f = open(credentials_path)
        self.creds = json.load(f)
        self.base = self.creds[stage]['host'] + ':' + self.creds[stage]['port'] \
                    + '/api/v1'
        self.get_collection_time_path = self.base + '/get_cxt/?ax_idx='
        f.close()

    def get_collection_time(self, ax_idx):
        api_endpoint = self.get_collection_time_path + str(ax_idx)
        response = requests.get(api_endpoint)
        return json.loads(response.text)
