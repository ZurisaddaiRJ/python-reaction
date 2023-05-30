# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json

uri = 'mongodb+srv://zurisaddairj:mongo_Atlas.1102@books.hgiw0w5.mongodb.net/?retryWrites=true&w=majority'
#uri = 'mongodb://127.0.0.1:27017/books'

# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.books
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")

consumer = KafkaConsumer('reactions',bootstrap_servers=[
    #'localhost:9092'])
    'book-kafka-0.book-kafka-headless.zurisaddairj.svc.cluster.local:9092'])
  
# Parse received data from Kafka

#Create memee_summary and insert groups into MongoDB


for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    userId = record["userId"]
    objectId = record["objectId"]
    reactionId = record["reactionId"]
    

    #count

    # Create dictionary and ingest data into MongoDB
    try:
       reaction_rec = {
         'userId': userId,
         'objectId': objectId,
         'reactionId': reactionId
       }
       print (reaction_rec)
       reaction_id = db.books_reactions.insert_one(reaction_rec)
       print("Data inserted with record ids", reaction_id)
    except:
       print("Could not insert into MongoDB")

    try:
       agg_result=db.books_reactions.aggregate(
       [{
         "$group" : 
         {  "_id" : {
               "objectId": "$objectId",
               "reactionId": "$reactionId"
            }, 
            "n"    : {"$sum": 1}
         }}
       ])
       db.books_summary.delete_many({})
       for i in agg_result:
         print(i)
         summary_id= db.books_summaryreactions.insert_one(i)
         print("Reaction inserted  with record  ids",summary_id)
    except Exception as e:
       print(f'group by caught {type(e)}: ')
       print(e)
