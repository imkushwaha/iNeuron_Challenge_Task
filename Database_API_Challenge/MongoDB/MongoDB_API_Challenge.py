import csv
from flask import Flask, request, jsonify,render_template
import pymongo
import logging as lg
import pandas as pd
import os


app = Flask(__name__)

@app.route('/create_connection', methods = ["POST"])

def create_connection():

    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']

        try:

            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            lg.info(f"Local Host Url {LocalHostUrl} has tried to make a connection !!")

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info(client)
            lg.info("Connection successfull !!")

            return jsonify(client.list_database_names())

        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/create_collection', methods=["POST"])

def create_collection():
    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']
        Database = request.json['Database']
        CollectionName = request.json['CollectionName']

        try:

            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info(f"Collection creation started at {Database}")

            db = client[f"{Database}"]
            my_collection = db[f"{CollectionName}"]

            lg.info(f"Collection: {CollectionName} under databse {Database} successfully created !! ")

            return f"Collection: {CollectionName} under databse {Database} successfully created !! "

        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/insert_one', methods=["POST"])
def insert_one():

    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']
        Database = request.json['Database']
        CollectionName = request.json['CollectionName']
        Record = request.json['Record']

        try:

            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info("insertion started......")

            db = client[f"{Database}"]

            my_collection = db[f"{CollectionName}"]

            my_collection.insert_one(Record)

            lg.info(f"Record get successfully inserted into collection {CollectionName} under database {Database}")

            return f"Record: {Record} get successfully inserted into collection {CollectionName} under database {Database}"

        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/insert_many', methods=["POST"])
def insert_many():

    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']
        Database = request.json['Database']
        CollectionName = request.json['CollectionName']
        Records = request.json['Records']

        try:

            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info("insertion started......")

            db = client[f"{Database}"]

            my_collection = db[f"{CollectionName}"]

            my_collection.insert_many(Records)

            lg.info(f"Records get successfully inserted into collection {CollectionName} under database {Database}")

            return f"Records: {Records} get successfully inserted into collection {CollectionName} under database {Database}"

        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/update_one', methods=["POST"])
def update_one():

    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']
        Database = request.json['Database']
        CollectionName = request.json['CollectionName']
        Query = request.json['Query']
        NewValue = request.json['NewValue']

        try:

            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info("Updation of record get started......")

            db = client[f"{Database}"]

            my_collection = db[f"{CollectionName}"]

            my_collection.update_one(Query, NewValue)

            lg.info(f"Record get successfully updated !!")

            return f"Record get successfully updated from {Query} to {NewValue}"

        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/delete_one', methods=["POST"])
def delete_one():

    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']
        Database = request.json['Database']
        CollectionName = request.json['CollectionName']
        Query = request.json['Query']


        try:

            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info("Deleting of record get started......")

            db = client[f"{Database}"]

            my_collection = db[f"{CollectionName}"]

            my_collection.delete_one(Query)

            lg.info(f"Record get successfully deleted !!")

            return f"Record get successfully deleted from collection {CollectionName}"

        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/bulk_insert', methods=["POST"])
def bulk_insert():

    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']
        Database = request.json['Database']
        CollectionName = request.json['CollectionName']
        filename = request.json['filename']
        Col1 = request.json['col1']
        Col2 = request.json['col2']
        Col3 = request.json['col3']
        Col4 = request.json['col4']

        try:

            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info("Bulk insertion get started......")

            with open(f"{filename}") as data:
                records = csv.reader(data, delimiter = ",")
                next(records)
                main_list = []
                record_count = 0
                for record in records:

                    my_dict = {}

                    colvalue1 = record[0]
                    colvalue2 = record[1]
                    colvalue3 = record[2]
                    colvalue4 = record[3]
                    my_dict = {Col1: colvalue1, Col2: colvalue2, Col3: colvalue3, Col4: colvalue4}
                    main_list.append(my_dict)

                    record_count = record_count + 1

            db = client[f"{Database}"]

            my_collection = db[f"{CollectionName}"]

            my_collection.insert_many(main_list)

            lg.info(f"Bulk insertion get successfully completed !!")
            lg.info(f"Total {record_count} records get inserted.")

            return f"Bulk insertion get successfully completed at collection {CollectionName} under DB {Database}"

        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/download_data', methods = ["POST"])
def download_data():

    if (request.method == 'POST'):
        LocalHostUrl = request.json['LocalHostUrl']
        Database = request.json['Database']
        CollectionName = request.json['CollectionName']

        try:
            lg.basicConfig(filename="MongoDB_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            client = pymongo.MongoClient(LocalHostUrl)

            lg.info("Downloading of records get started......")

            db = client[f"{Database}"]

            my_collection = db[f"{CollectionName}"]

            main_list = []
            no_of_records = 0
            for data in my_collection.find():
                no_of_records = no_of_records + 1
                main_list.append(data)

            df = pd.DataFrame(main_list)
            name = f"{CollectionName}_data.csv"
            df.to_csv(name, index=False)

            lg.info(f"Data successfully downloaded at {os.getcwd()} with name {name}")
            lg.info(f"Total number of records downloaded: {no_of_records}")
            lg.info("<<<<<<<TASK COMPLETED SUCCESSFULLY>>>>>>>")


            return f"Data successfully downloaded at {os.getcwd()} with name {name}"

        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))



if __name__ == "__main__":
    app.run(debug = True)