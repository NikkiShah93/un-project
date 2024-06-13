from pymongo import MongoClient
from utils import get_file

client = MongoClient("localhost", 27017)

def create_meta(PATH = '\\data\\table-list.json',
                 dbname = 'test_db', 
                 parent=True,
                 meta_name = "mdata",
                 client=client):
    table_list = get_file(PATH, parent=parent)
    ## create a database
    table_db = client[dbname]
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
    pass

