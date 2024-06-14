from pymongo import MongoClient
from bson.objectid import ObjectId
from utils import get_file
import time    


client = MongoClient("localhost", 27017)

def create_meta(PATH = '\\data\\table-list.json',
                 db_name = 'test_db', 
                 parent=True,
                 meta_name = "mdata",
                 client=client):
    table_list = get_file(PATH, parent=parent)
    ## create a database
    table_db = client[db_name]
    if meta_name in table_db.list_collection_names():
        table_db[meta_name].drop()
    ## creating the metadata table
    mdata = table_db.mdata
    for table, val in table_list.items():
        mdata.insert_one({"name":table, "values":val}) 
    return mdata.count_documents({})

def get_meta(table_name = "mdata",
             db_name="test_db",
             client=client):
    try:
        return client[db_name][table_name].find({})
    except Exception as e:
        return e

def create_table(db_name = "test_db",
                 table_name = "test_table",
                 client = client,
                 drop_if_exist = False):
    try:
        if table_name in client[db_name].list_collection_names() and drop_if_exist:
            client[db_name][table_name].drop()
        table = client[db_name][table_name]
        return "success"
    except Exception as e:
        return e
    
def create_doc(
        db_name="test_db",
        table_name="test_data",
        object_id = None,
        tags=None,
        data=None,
        client=client
):
    if data is not None:
        try:
            if object_id is not None:
                object_id = ObjectID(object_id)
            doc = {
                '_id': object_id,
                'author': 'mongo_manager',
                'date': int(time.time()),
                'tags': tags,
                'data': data
            }
            client[db_name][table_name].insert_one(doc)
            return "success"
        except Exception as e:
            return e

        

def insert_data(db_name = "test_db",
                 table_name = "test_table",
                 client = client,
                 data=None):
    if data is not None:
        try:
            table = client[db_name][table_name]
            table.insert_one(data)
            return "success"
        except Exception as e:
            return e

def get_data(db_name = "test_db",
            table_name = "test_table",
            client = client,
            filters = {}):
    try:
        table = client[db_name][table_name]
        return table.find(filters)
    except Exception as e:
        return e
