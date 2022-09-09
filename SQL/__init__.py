import mysql.connector
from mysql.connector import Error
import json

credentials_path = './creds/ml_resulting_creds.json'

class SQLAgent():
    def __init__(self):
        f = open(credentials_path)
        self.creds = json.load(f);
        f.close()

    def create_connection(self, db):

        try:
            connection = mysql.connector.connect(
                host = self.creds["host"],
                user = self.creds["user"],
                passwd = self.creds["password"],
                database = db
            )
            print("Connection to " + self.creds["host"] + " successfull.")
            return connection

        except Error as e:
            print("Error: " + e)


    
