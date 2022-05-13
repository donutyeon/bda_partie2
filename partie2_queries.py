from asyncio.windows_events import NULL
from pymongo import MongoClient
import json
import pprint

pp = pprint.PrettyPrinter(indent=4)

client = MongoClient('localhost', 27017)
print('client créé')
db = client['BDD']
world = db['world']
#question 1 : nombre de pays dans la collection.
q1=world.count_documents({})
print('nombre de pays : ',nb_pays)
#question 2 : lister les differents continents.
q2=world.distinct("Continent")
print("***********************************\n")
print('les différents continents: ',q2)
#question 3: lister les information de l'algérie
q3=list(world.find({"Name":"Algeria"},{"_id":0}))
print("***********************************\n")
print('les informations de l\'Algérie: \n')
pp.pprint(q3)
#question 4: les pays africains dont la population est inferieure a 100k
q4=list(world.find({"Continent":"Africa","Population" : {"$lt":100000}},{"_id":0,"Name":1}))
print("***********************************\n")

print('pays africains :\n',q4)
#question 5: les pays independants du continent oceanique
q5=list(world.find({"Continent":"Oceania","IndepYear" : {"$ne":"NA"}},{"_id":0,"Name":1}))
print("***********************************\n")
print('pays independants :\n',q5)
#question 6 : le continent avec la plus grande surface
q6=list(world.aggregate([
    {"$group" :
        {
        "_id":"$Continent","Surface" :{"$sum":"$SurfaceArea"}
        }
    },
    {"$sort":{"Surface":-1}},
    {"$limit":1}
]))
print("***********************************\n")
print('continent ayant la plus grande surface :\n',q6);
#question 7: par continent , le nombre de pays , population ,nombre de pays independants , cant figure out independant countries yet sautitha
q7=list(world.aggregate([
    {
        "$group":
        {
            "_id":"$Continent","Nombre de pays":{"$sum":1},"Population":{"$sum":"$Population"},
            "Nombre pays independant":{"$sum":{"$cond":{"if":{"$eq":["$IndepYear","NA"]},"then":0,"else":1}}}
        }
    }
]))
print("***********************************\n",q7)
#question 8:la population totale des villes d’Algérie
q8=list(world.aggregate([
    {
        "$match":
        {
            "Name" : "Algeria"
        }
    },
    {
        "$unwind":"$Cities"
    },
    {
        "$group":{
            "_id":NULL,"population totale des villes":{"$sum":"$Cities.Population"}
        }
    }
]))
print("***********************************\n",q8)
#q9 : capitale (uniquement nom de la ville et population) d’Algérie
q9=list(world.find({"Name":"Algeria"},{"Capital.Name":1,"Capital.Population":1,"_id":0}))
print("***********************************\n",q9)
#q10: les langues parlées dans plus de 15 pays
q10= list(world.aggregate([
    {
        "$unwind":"$OffLang"
    },
    {
        "$group":
        {
            "_id":"$OffLang.Language","nb":{"$sum":1}
        }
    },
    {
        "$match":
        {
            "nb":{"$gt":15}
        }
    }
]))
print("***********************************\n",q10)
#q11:pour chaque pays le nombre de villes (pour les pays ayant au moins 100 villes), en les
#triant par ordre décroissant du nombre de villes
q11=list(world.aggregate([
    {
        "$project":
        {
            "_id":0,
            "Name": 1,
            "Nombre de villes": { "$cond": { "if": { "$isArray": "$Cities" }, "then": { "$size": "$Cities" }, "else": "NA"} }
        }
    },
    {
        "$match":
        {
            "Nombre de villes":{"$gt":100}
        }
    },    
    {
        "$sort":{"Nombre de villes":-1}
    }
]))
print("***********************************\n",q11)
#les 10 villes les plus habitées, ainsi que leur pays, dans l’ordre décroissant de la population
q12=list(world.aggregate([
    {
        "$unwind":"$Cities"
    },
    {
        "$project":
        {
            "Cities.Name":1,"Cities.Population":1,"Name":1,"_id":0
        }
    },
    {
        "$sort":{"Cities.Population":-1}
    },
    {
        "$limit":10
    }
]))
print("***********************************\n",q12)
#q13:les pays pour lesquels l’Arabe est une langue officielle
q13=list(world.aggregate([
    {"$unwind":"$OffLang"},
    {
        "$project":
        {
            "Name":1,"_id":0,"Arabic":{"$eq":[ "Arabic", "$OffLang.Language"]}
        }
    },
    {
        "$match":
        {
            "Arabic":True
        }
    }
]))
print("***********************************\n",q13)
#q14: Lister les 5 pays avec le plus de langues parlées
q14=list(world.aggregate([
    {
        "$project":
        {
            "Name":1,
            "Nombre de langue off": { "$cond": { "if": { "$isArray": "$OffLang" }, "then": { "$size": "$OffLang" }, "else": 0} },
            "Nombre de langue non off": { "$cond": { "if": { "$isArray": "$NotOffLang" }, "then": { "$size": "$NotOffLang" }, "else": 0} }
        }
    },
    {
        "$project":
        {
            "_id":0,"Name":1,"Nombre de langues":{"$add":["$Nombre de langue off","$Nombre de langue non off"]}
        }
    },
    {
        "$sort":{"Nombre de langues":-1}
    },
    {"$limit":5}
]))
print("***********************************\n",q14)
#q15 : les pays pour lesquels la somme des populations des villes est supérieure à la population du pays.
q15=list(world.aggregate([
    {"$unwind":"$Cities"},
    {
        "$group":
        {
            "_id":"$Name","Population des villes":{"$sum":"$Cities.Population"},"Population":{"$first":"$Population"}
        }
    },
    {
        "$project":
        {
            "_id":1,"Population des villes":1,"Population":1,
            "population villes superieure":{"$cond":{"if":{"$gt":["$Population des villes","$Population"]},"then":1,"else":0}}
        }
    },
    {
        "$match":
        {
            "population villes superieure":{"$eq":1}
        }
    }
]))
print("***********************************\n",q15)
