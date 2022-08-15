import pymongo
import json

credentials_path = './creds/mongo_creds.json'

class MongoAgent():

    def __init__(self):
        f = open(credentials_path)
        self.creds = json.load(f)
        f.close()

    def create_connection(self):
        connection_string = "mongodb://" + self.creds["user"] \
                            + ":" + self.creds["password"]  \
                            + "@" + self.creds["host"] \
                            + ":" + self.creds["port"]


        try:
            self.client = pymongo.MongoClient(connection_string)
            print("Mongo connection to " + self.creds["host"] + " successfull.")
            return self.client
        except pymongo.errors.ConnectionFailure as e:
            print("Error: " + e)


    def close(self):
        print("Closing mongo connection to " + self.creds["host"])
        self.client.close()
