import ifcopenshell
import datetime
from bson.dbref import DBRef
from pymongo import MongoClient
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--uri', type=str, help='MongoDB URI String', required=True)
parser.add_argument('--db', type=str, help='MongoDB database', required=True)
parser.add_argument('--col', type=str, help='MongoDB collection', required=True)
parser.add_argument('--batch', type=str, help='Batch size', required=False, default=5000)
parser.add_argument('--file', type=str, help='Input file location', required=True)
args = parser.parse_args()

t1 = datetime.datetime.now()
doc_count = 0
batch = []

mongo_uri = args.uri
database_name = args.db
collection_name = args.col
batch_size = args.batch
dest_file_path = args.file 

def parse_value(v):
    global collection_name

    if isinstance(v, ifcopenshell.entity_instance):
        if v.id() == 0:
            return v.get_info()

        return  DBRef(collection_name, v.id())
    elif isinstance(v, tuple):
        return [parse_value(item) for item in v]
    else:
        return v



ifc_file = ifcopenshell.open(dest_file_path)
ifc_iter = iter(ifc_file)

connection = MongoClient(mongo_uri)
col = connection[database_name][collection_name]

try: 
    while True:
            ifc_dict = next(ifc_iter).get_info()

            mongo_dict = {k : parse_value(v) for k, v in ifc_dict.items()}
            mongo_dict['_id'] = mongo_dict['id']

            batch.append(mongo_dict)

            doc_count+=1

            if len(batch) == batch_size:
                col.insert_many(batch)
                batch = []

                print("-----------------------")
                print("- Import count: {}".format(str(doc_count)))


except StopIteration:
    if len(batch) > 0:
        col.insert_many(batch)

t2 = datetime.datetime.now()
print("- Elapsed time: {}".format(str(t2 - t1)))
print("- Done.")
 

