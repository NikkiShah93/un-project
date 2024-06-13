from pymongo import MongoClient
from utils import get_file

client = MongoClient("localhost", 27017)

def create_meta(PATH = '\\data\\table-list.json',
                 dbname = 'test_db', 
                 parent=True,
                 client=client):
    table_list = get_file(PATH, parent=parent)
    ## create a database
    table_db = client[dbname]
    if "mdata" in table_db.list_collection_names():
        table_db['mdata'].drop()
    ## creating the metadata table
    mdata = table_db.mdata
    for table, val in table_list.items():
        mdata.insert_one({"name":table, "values":val}) 
    return mdata.count_documents()

