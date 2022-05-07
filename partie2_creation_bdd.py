from pymongo import MongoClient
import json
client = MongoClient('localhost', 27017)
print('client créé')
db = client['BDD']
world = db['world']
with open("world-mongodb.json",encoding="utf-8") as file : 
    file_data=json.load(file)
print("got the file's elements")
if(isinstance(file_data, list)):
    print("inserting many...")
    world.insert_many(file_data)
    print("data inserted")
else :
    world.insert_one(file_data)
    print("data not inserted")