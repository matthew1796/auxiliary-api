import json
import requests
import csv

credentials_path = './creds/Mockaroo_creds.json'
fake_data_path = './fake_data/fake_mrn_data.csv'

class MockarooAgent ():
    def __init__(self):
        f = open(credentials_path)
        self.creds = json.load(f)
        self.new_patients_path = '/new_patients'
        f.close();

    def generate_fake_data(self, num_mrns):
        api_endpoint = self.creds['base'] +  \
                       self.new_patients_path + '/' + str(num_mrns) + \
                       '?key=' + self.creds['apiKey']
        response = requests.get(api_endpoint)
        print(api_endpoint)

        with open(fake_data_path, 'w') as temp_file:
            temp_file.writelines(response.text)

        with open(fake_data_path, 'rU') as  temp_file:
            csv_reader = csv.reader(fake_data_path, dialect=csv.excel_tab)
