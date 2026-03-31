
from pymongo import MongoClient
import config
client = MongoClient(config.MONGO_URI)
db = client[config.DB_NAME]
print('DB:', config.DB_NAME)
print('OUTPUT_COLL:', config.OUTPUT_COLL)
print('INPUT_COLL:', config.INPUT_COLL)
print()
for col in db.list_collection_names():
    count = db[col].count_documents({})
    print(f'  {col} : {count} documents')
client.close()