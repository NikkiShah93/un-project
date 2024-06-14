## importing the get data function from utils
from utils import get_data
import mongo_manager

if __name__=="__main__":
    dbname = 'hazards-waste'
    data = {}
    ## creating the metadata table
    # counts = mongo_manager.create_meta(PATH = '\\data\\table-list.json',
    #              db_name = dbname, 
    #              parent=True)
    # print(counts)
    # if counts > 0:
    #     print("The meta table was created!")
    #     print(f"The count of available docs: {counts}")
    # ## getting the meta data from mongodb
    # table_list = mongo_manager.get_meta(db_name=dbname, 
    #                                     table_name="mdata")
    # ## getting the data for that table
    # for i, t in enumerate(table_list):
    #     if t.get('values', {}).get('url', None) is not None:
    #         name = t.get('name',f'table_{i}')
    #         url = t.get('values', {}).get('url', '')
    #         data[name] = get_data(url, name)
    # ## creating the table 
    # for table in data:
    #     response = mongo_manager.create_table(db_name=dbname,
    #                                            table_name=table, 
    #                                            drop_if_exist= True)
    #     if response == 'success':
    #         ## now we can insert data into our table
    #         data_response = mongo_manager.insert_data(db_name=dbname,
    #                                                   table_name=table,
    #                                                   data=data[table])
    #         print(data_response)
    ## testing to see if the data is in the target table
    table_list = mongo_manager.get_meta(db_name=dbname,
                                        table_name="mdata")
    full_data = {}
    for table in table_list:
        full_data[table['name']] = list(mongo_manager.get_data(
            db_name=dbname,
            table_name=table['name'],
            ))

    print(full_data)
    new_set = {}
    for obj, vals in full_data.items():
        new_set[obj] = {}
        for tag in vals:
            for col in tag:
                if col != '_id':
                    print(tag)
                    print(f"{type(tag)}\n\nEND of TAG\n", col)
                    new_set[obj][col] = tag[col]
    print(new_set)
        # for d in obj:
        #     print(d)
    # ## getting the data for each table
    # for key in table_list:
    #     url = table_list[key]['url']
    #     table_list[key]['data'] = get_data(url, table_name=key)