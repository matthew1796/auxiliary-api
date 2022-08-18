import pandas as pd
import requests
import json
from csv import writer
from random import randint
from ELIS import ELISAgent











def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)




def generate_json_obj(token, panelID, row):

    specimenId = 'XX-98765' + str(random_with_N_digits(4))
    post_body = {
        "token" : token,
        "patient": {
		       "firstName": row['first_name'],
		       "lastName": row['last_name'],
		       "birthDate": row['dob'],
		       "race": row['race'],
		       "gender": row['sex'],
		       "email": row['email'],
		       "address": {
			           "phone": row['phone'],
			           "address1": row['address1'],
			           "city": row['city'],
			           "country": 'United States',
			           "state": row['state'],
		               "zipCode": row['zip']
		       }
	       },
	       "lab": "Default",
	       "productLine": "COVID-19",
	       "specimenType": "Nasal Swab",
	       "providerNPI": "1740685387-CC",
	       "specimenId": specimenId,
	       "panels": [
		         panelID
	       ],
	       "diagCodes": [
	       ],
	       "documents": [
	       ],
	       "insurances": [
	       ]
           }


    return post_body
