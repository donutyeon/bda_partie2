from PyQt5 import QtWidgets, uic
import sys
from asyncio.windows_events import NULL
from pymongo import MongoClient
import json
import pprint

from sympy import Q


client = MongoClient('localhost', 27017)
print('client créé')
db = client['BDD']
world = db['world']

class Ui(QtWidgets.QMainWindow):
        def __init__(self):
                super(Ui, self).__init__()
                uic.loadUi('main.ui', self)

                ####################### declare widgets
                ### execute button
                self.execute =  self.findChild(QtWidgets.QPushButton, 'execute_button')
                self.execute.clicked.connect(self.executeClickListener)
                ### combo box
                self.queries =  self.findChild(QtWidgets.QComboBox, 'queries')
                ### query result
                self.results =  self.findChild(QtWidgets.QTextEdit, 'query_results')
                self.results.setReadOnly(True)


        ########################################################################################################
        #############################      BUTTON LISTENERS       ##############################################

        def executeClickListener(self):
                question = self.queries.currentText()
                question = question[len("question"): question.index(":")].strip()
                if(question == '1'):
                        q1=world.count_documents({})
                        self.results.setText(pprint.pformat(q1, indent=4))
                if(question == '2'):
                        q2=world.distinct("Continent")
                        q2 = pprint.pformat(q2, indent=4)
                        q2 = q2.replace("]", "")
                        q2 = q2.replace("[", "")
                        q2 = q2.replace("\'", "")
                        q2 = q2.replace(",", "")
                        q2 = " "+q2
                        self.results.setText(q2)
                if(question == '3'):
                        q3=list(world.find({"Name":"Algeria"},{"_id":0}))
                        q3 = pprint.pformat(q3, indent=4).replace("{", "")
                        q3 = q3.replace("}", "")
                        q3 = q3.replace("]", "")
                        q3 = q3.replace("[", "")
                        q3 = q3.replace("\'", "")
                        q3 = q3.replace(",", "")
                        q3 = " "+q3
                        self.results.setText(q3)
                if(question == '4'):
                        q4=list(world.find({"Continent":"Africa","Population" : {"$lt":100000}},{"_id":0,"Name":1}))
                        q4=pprint.pformat(q4, indent=4).replace("{", "")
                        q4=q4.replace("}", "")
                        q4=q4.replace("]", "")
                        q4=q4.replace("[", "")
                        q4=q4.replace("\'", "")
                        q4 = q4.replace(",", "")
                        q4 = " "+q4
                        self.results.setText(q4)
                if(question == '5'):
                        q5=list(world.find({"Continent":"Oceania","IndepYear" : {"$ne":"NA"}},{"_id":0,"Name":1}))
                        q5=pprint.pformat(q5, indent=4).replace("{", "")
                        q5=q5.replace("}", "")
                        q5=q5.replace("]", "")
                        q5=q5.replace("[", "")
                        q5=q5.replace("\'", "")
                        q5 = q5.replace(",", "")
                        q5 = " "+q5
                        self.results.setText(q5)
                if(question == '6'):
                        q6=list(world.aggregate([
                                {"$group" :
                                        {
                                                "_id":"$Continent","Surface" :{"$sum":"$SurfaceArea"}
                                        }
                                },
                                {"$sort":{"Surface":-1}},
                                {"$limit":1}
                        ]))
                        q6=pprint.pformat(q6, indent=4).replace("{", "")
                        q6=q6.replace("}", "")
                        q6=q6.replace("]", "")
                        q6=q6.replace("[", "")
                        q6=q6.replace("\'", "")
                        q6 = q6.replace(",", "")
                        q6 = " "+q6
                        self.results.setText(q6)
                if(question == '7'):
                        q7=list(world.aggregate([
                                {"$group":
                                        {
                                                "_id":"$Continent","Nombre de pays":{"$sum":1},"Population":{"$sum":"$Population"},
                                                "Nombre pays independant":{"$sum":{"$cond":{"if":{"$eq":["$IndepYear","NA"]},"then":0,"else":1}}}
                                        }
                                }
                        ]))
                        q7=pprint.pformat(q7, indent=4).replace("{", "")
                        q7=q7.replace("}", "")
                        q7=q7.replace("]", "")
                        q7=q7.replace("[", "")
                        q7=q7.replace("\'", "")
                        q7 = q7.replace(",", "")
                        q7 = " "+q7
                        self.results.setText(q7)
                if(question == '8'):
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
                                },
                                {
                                        "$project":
                                        {
                                                "_id":0
                                        }
                                }
                                ]))
                        q8=pprint.pformat(q8, indent=4).replace("{", "")
                        q8=q8.replace("}", "")
                        q8=q8.replace("]", "")
                        q8=q8.replace("[", "")
                        q8=q8.replace("\'", "")
                        q8 = q8.replace(",", "")
                        q8 = " "+q8
                        self.results.setText(q8)
                if(question == '9'):
                        q9=list(world.find({"Name":"Algeria"},{"Capital.Name":1,"Capital.Population":1,"_id":0}))
                        q9=pprint.pformat(q9, indent=4).replace("{", "")
                        q9=q9.replace("}", "")
                        q9=q9.replace("]", "")
                        q9=q9.replace("[", "")
                        q9=q9.replace("\'", "")
                        q9 = q9.replace(",", "")
                        q9 = " "+q9
                        self.results.setText(q9)
                if(question == '10'):
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
                        q10=pprint.pformat(q10, indent=4).replace("{", "")
                        q10=q10.replace("}", "")
                        q10=q10.replace("]", "")
                        q10=q10.replace("[", "")
                        q10=q10.replace("\'", "")
                        q10 = q10.replace(",", "")
                        q10 = " "+q10
                        self.results.setText(q10)
                if(question == '11'):
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
                        q11=pprint.pformat(q11, indent=4).replace("{", "")
                        q11=q11.replace("}", "")
                        q11=q11.replace("]", "")
                        q11=q11.replace("[", "")
                        q11=q11.replace("\'", "")
                        q11 = q11.replace(",", "")
                        q11 = " "+q11
                        self.results.setText(q11)
                if(question == '12'):
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
                        q12=pprint.pformat(q12, indent=4).replace("{", "")
                        q12=q12.replace("}", "")
                        q12=q12.replace("]", "")
                        q12=q12.replace("[", "")
                        q12=q12.replace("\'", "")
                        q12 = q12.replace(",", "")
                        q12 = " "+q12
                        self.results.setText(q12)
                if(question == '13'):
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
                        q13=pprint.pformat(q13, indent=4).replace("{", "")
                        q13=q13.replace("}", "")
                        q13=q13.replace("]", "")
                        q13=q13.replace("[", "")
                        q13=q13.replace("\'", "")
                        q13 = q13.replace(",", "")
                        q13 = " "+q13
                        self.results.setText(q13)
                if(question == '14'):
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
                        q14=pprint.pformat(q14, indent=4).replace("{", "")
                        q14=q14.replace("}", "")
                        q14=q14.replace("]", "")
                        q14=q14.replace("[", "")
                        q14=q14.replace("\'", "")
                        q14 = q14.replace(",", "")
                        q14 = " "+q14
                        self.results.setText(q14)
                if(question == '15'):
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
                        q15=pprint.pformat(q15, indent=4).replace("{", "")
                        q15=q15.replace("}", "")
                        q15=q15.replace("]", "")
                        q15=q15.replace("[", "")
                        q15=q15.replace("\'", "")
                        q15 = q15.replace(",", "")
                        q15 = " "+q15
                        self.results.setText(q15)
                
                
                

app = QtWidgets.QApplication(sys.argv)
window = Ui()
window.show()
app.exec_()

