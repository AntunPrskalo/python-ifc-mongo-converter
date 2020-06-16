import ifcopenshell
from bson.dbref import DBRef
from pymongo import MongoClient
from bson.dbref import DBRef
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--uri', type=str, help='MongoDB URI String', required=True)
parser.add_argument('--db', type=str, help='MongoDB database', required=True)
parser.add_argument('--col', type=str, help='MongoDB collection', required=True)
parser.add_argument('--batch', type=str, help='Batch size', default=20000)
parser.add_argument('--file', type=str, help='Destination file location', required=True)
args = parser.parse_args()

t1 = datetime.datetime.now()
processed = 0
doc_count = None
cache_dict = {}
refetch_entitiy_ids = []
missing_ref_entities = []

mongo_uri = args.uri
database_name = args.db
collection_name = args.col
batch_size = args.batch 
dest_file_path = args.file 

ifc_file = ifcopenshell.open()

def parse_mongo_entities(entities):
    global ifc_file
    global cache_dict

    for mongo_entity in entities:
        if mongo_entity['_id'] in cache_dict:
            continue

        parsed_entity = parse_mongo_entity(mongo_entity)

        if parsed_entity  is not None:
            ifc_entity = ifcopenshell.create_entity(parsed_entity['type'], **{k : v for k, v in parsed_entity.items() if k not in ['id', '_id', 'type']})        
            ifc_file.add(ifc_entity)
            cache_dict[parsed_entity['_id']] = ifc_entity

def parse_mongo_entity(entity):
    global cache_dict

    is_complete = True
    for key, val in entity.items():
        if isinstance(val, DBRef):
            if val.id not in cache_dict:
                refetch_entitiy_ids.append(val.id)
                is_complete = False
            else:
                entity[key] = cache_dict[val.id]
        elif isinstance(val, dict):
            entity[key] = ifcopenshell.create_entity(val['type'], **{k : v for k, v in val.items() if k not in ['id', 'type']})  
        elif isinstance(val, list):
            for i in range(len(val)):
                if isinstance(val[i], DBRef):
                    if val[i].id not in cache_dict:
                        refetch_entitiy_ids.append(val[i].id)
                        is_complete = False
                    else:
                        entity[key][i] = cache_dict[val[i].id]  
                elif isinstance(val[i], dict):
                    entity[key][i] = ifcopenshell.create_entity(val[i]['type'], **{k : v for k, v in val[i].items() if k not in ['id', 'type']})  

    if is_complete:
        return entity

    missing_ref_entities.append(entity)        

connection = MongoClient(mongo_uri)
col = connection[database_name][collection_name]

# ?? possible error
doc_count = col.count_documents({})

while processed <= doc_count:
    mongo_entities = col.find({}, limit=batch_size, skip=processed, sort=[('_id', 1)])
    parse_mongo_entities(mongo_entities)

    while len(refetch_entitiy_ids) != 0: 
        refetched_entities = col.find({"_id": {"$in": refetch_entitiy_ids}}, sort=[('_id', 1)])
        refetch_entitiy_ids = []
        parse_mongo_entities(refetched_entities)
        
    parse_mongo_entities(missing_ref_entities)
    missing_ref_entities = []
    processed += batch_size

    print("-----------------------")
    print("- Export count: {}".format(str(processed)))

ifc_file.write(dest_file_path)

t2 = datetime.datetime.now()
print("- Elapsed time: {}".format(str(t2 - t1)))
print("- Done.")