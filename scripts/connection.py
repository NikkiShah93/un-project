from pymongo import MongoClient
from utils import get_file

client = MongoClient("localhost", 27017)
PATH = '../data/table_list.json'
table_list = get_file(PATH)

## create a database
table_db = client.hazard_waste

# table_list = {"hazardous-waste-treated-or-disposed":
#               {'url':'https://data.un.org/Data.aspx?d=ENV&f=variableID:1780&c=2,3,4,5&s=countryName:asc,yr:desc&v=pagenum',
#              'data':None},
#              "total-amount-of-municipal-waste-collected":
#              {'url':'https://data.un.org/Data.aspx?d=ENV&f=variableID:1814&c=2,3,4,5&s=countryName:asc,yr:desc&v=pagenum',
#              'data':None},
#              "hazardous-waste-landfilled":
#              {'url':'https://data.un.org/Data.aspx?d=ENV&f=variableID:1841&c=2,3,4,5&s=countryName:asc,yr:desc&v=pagenum',
#              'data':None},
#               "total-population-served-by-municipal-waste-collection":
#              {'url':'https://data.un.org/Data.aspx?d=ENV&f=variableID:1878&c=2,3,4,5&s=countryName:asc,yr:desc&v=pagenum',
#              'data':None},
#               "hazardous-waste-recycled":
#              {'url':'https://data.un.org/Data.aspx?d=ENV&f=variableID:2573&c=2,3,4,5&s=countryName:asc,yr:desc&v=pagenum',
#              'data':None},
#               "hazardous-waste-incinerated":
#              {'url':'https://data.un.org/Data.aspx?d=ENV&f=variableID:2574&c=2,3,4,5&s=countryName:asc,yr:desc&v=pagenum',
#              'data':None},
#               "hazardous-waste-generated":
#              {'url':'https://data.un.org/Data.aspx?d=ENV&f=variableID:2830&c=2,3,4,5&s=countryName:asc,yr:desc&v=pagenum',
#              'data':None},
#              }
if "mdata" in table_db.list_collection_names():
    table_db['mdata'].drop()

## creating the metadata table
mdata = table_db.mdata
for table, val in table_list.items():
    mdata.insert_one({"name":table, "values":val}) 
for data in mdata.find():
    print(data)