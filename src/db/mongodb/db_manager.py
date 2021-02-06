import pymongo
from pymongo import MongoClient

class MongoManager():
    def __init__(self, configurator) -> None:
        self.configurator = configurator
        self.host = configurator['databases']['mongodb']['dev']['host']
        self.port = configurator['databases']['mongodb']['dev']['port']

    def insert_data(self, data_dict:dict):
        conn = MongoClient(self.host, self.port)
        db = conn.mydb
        db.myset.insert(data_dict)
        conn.close()

    def query_last_row(self, uid:str)->dict:
        conn = MongoClient(self.host, self.port)
        db = conn.mydb
        cur=db.myset.find({'uid':uid}).sort('timestamp',pymongo.DESCENDING).limit(1)
        conn.close()

        return cur[0]
    
    def insert_row(self, x):
        if x is None or len(x)<1:
            return
        data_list=x.split(',')
        self.insert_data({
                    'timestamp': data_list[0],
                    'uid': data_list[1],
                    'heart_rate': data_list[2],
                    'steps': data_list[3]
        })