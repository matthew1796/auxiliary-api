import urllib.parse
from os.path import dirname, abspath, join
import pymongo
import json

__here__ = abspath(dirname(__file__))
creds = json.load(open(join(__here__, 'documentdb_creds.json'), 'r'))


class MongoClient:
    def __init__(self, database):
        db = creds[database]
        self.username = urllib.parse.quote_plus(db['username'])
        self.password = urllib.parse.quote_plus(db['password'])
        self.host = db['host']
        self.port = db['port']
        self.client = None

    def __enter__(self):
        self.client = pymongo.MongoClient(f'mongodb://{self.username}:{self.password}@{self.host}:{self.port}')
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
