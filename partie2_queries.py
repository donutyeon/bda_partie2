from pymongo import MongoClient
import json
client = MongoClient('localhost', 27017)
print('client créé')
db = client['BDD']
world = db['world']
#question 1 : nombre de pays dans la collection.
nb_pays=world.count_documents({})
print('nombre de pays : ',nb_pays)
#question 2 : lister les differents continents.
print('les différents continents: ',world.distinct("Continent"))
#question 3: lister les information de l'algérie
DZ_list=list(world.find({"Name":"Algeria"},{"_id":0}))
#print('les informations de l\'Algérie: \n',DZ_list)
#question 4: les pays africains dont la population est inferieure a 100k
print('pays africains :\n',list(world.find({"Continent":"Africa","Population" : {"$lt":100000}},{"_id":0,"Name":1})))
#question 5: les pays independants du continent oceaniquek
print('pays independants :\n',list(world.find({"Continent":"Oceania","IndepYear" : {"$ne":"NA"}},{"_id":0,"Name":1})))
#question 6 : le continent