
from flask import Flask, request, jsonify, render_template
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import cassandra
import csv
import logging as lg
import pandas as pd
import os


app = Flask(__name__)

@app.route('/create_connection', methods = ["POST"])

def create_connection():

    if (request.method == 'POST'):

        secure_connect_bundle = request.json['secure_connect_bundle']
        client_id = request.json['client_id']
        client_secret = request.json['client_secret']



        try:

            lg.basicConfig(filename="Cassandra_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            lg.info(f"Client_id: {client_id} & Client_secret: {client_secret} has tried to make a connection")

            cloud_config = {'secure_connect_bundle': secure_connect_bundle}

            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            row = session.execute("select release_version from system.local").one()

            lg.info(f"Connection Successfully Established at release_version: {row[0]}")
            return f"Connection Successfully Established at release_version: {row[0]}"




        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/create_table', methods = ["POST"])

def create_table():

    if (request.method == 'POST'):

        secure_connect_bundle = request.json['secure_connect_bundle']
        client_id = request.json['client_id']
        client_secret = request.json['client_secret']
        keyspace = request.json['keyspace']
        table_name = request.json['table_name']
        col0 = request.json['col0']
        col0_type = request.json['col0_type']
        col1 = request.json['col1']
        col1_type = request.json['col1_type']
        col2 = request.json['col2']
        col2_type = request.json['col2_type']
        col3 = request.json['col3']
        col3_type = request.json['col3_type']
        col4 = request.json['col4']
        col4_type = request.json['col4_type']


        try:

            lg.basicConfig(filename="Cassandra_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            cloud_config = {'secure_connect_bundle': secure_connect_bundle}

            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            lg.info("Table Creation started.....")

            session.execute(f"CREATE TABLE {keyspace}.{table_name}({col0} {col0_type} PRIMARY KEY, {col1} {col1_type}, {col2} {col2_type}, {col3} {col3_type},{col4} {col4_type});")

            lg.info(f"Table {table_name} successfully created under keyspace {keyspace}")
            return f"Table {table_name} successfully created under keyspace {keyspace}"


        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/insert_record', methods = ["POST"])

def insert_record():
    if (request.method == 'POST'):

        secure_connect_bundle = request.json['secure_connect_bundle']
        client_id = request.json['client_id']
        client_secret = request.json['client_secret']
        keyspace = request.json['keyspace']
        table_name = request.json['table_name']
        col0 = request.json['col0']
        col0_value = request.json['col0_value']
        col1 = request.json['col1']
        col1_value = request.json['col1_value']
        col2 = request.json['col2']
        col2_value = request.json['col2_value']
        col3 = request.json['col3']
        col3_value = request.json['col3_value']
        col4 = request.json['col4']
        col4_value = request.json['col4_value']


        try:

            lg.basicConfig(filename="Cassandra_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            cloud_config = {'secure_connect_bundle': secure_connect_bundle}

            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            lg.info("Insertion of record get started.....")

            session.execute(f"insert into {keyspace}.{table_name} ({col0}, {col1}, {col2}, {col3}, {col4}) values({col0_value}, {col1_value}, {col2_value}, {col3_value}, {col4_value});")

            lg.info(f"Record successfully get inserted in table {table_name} under keyspace {keyspace}")
            return f"Record successfully get inserted in table {table_name} under keyspace {keyspace}"


        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/update_record', methods = ["POST"])

def update_record():


    if (request.method == 'POST'):

        secure_connect_bundle = request.json['secure_connect_bundle']
        client_id = request.json['client_id']
        client_secret = request.json['client_secret']
        keyspace = request.json['keyspace']
        table_name = request.json['table_name']
        update_column = request.json['update_column']
        update_value = request.json['update_value']
        condition_column = request.json['condition_column']
        condition_value = request.json['condition_value']


        try:

            lg.basicConfig(filename="Cassandra_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',
                           datefmt='%Y-%m-%d %H:%M:%S')

            cloud_config = {'secure_connect_bundle': secure_connect_bundle}

            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            lg.info("Updating of record get started.....")

            session.execute(f"UPDATE {keyspace}.{table_name} SET {update_column} = {update_value} WHERE {condition_column} = {condition_value};")

            lg.info(f"Record successfully get updated in table {table_name} under keyspace {keyspace}")
            return f"Record successfully get updated in table {table_name} under keyspace {keyspace}"


        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/delete_record', methods = ["POST"])

def delete_record():

    if (request.method == 'POST'):

        secure_connect_bundle = request.json['secure_connect_bundle']
        client_id = request.json['client_id']
        client_secret = request.json['client_secret']
        keyspace = request.json['keyspace']
        table_name = request.json['table_name']
        primary_column = request.json['primary_column']
        primary_value = request.json['primary_value']


        try:

            lg.basicConfig(filename="Cassandra_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',
                           datefmt='%Y-%m-%d %H:%M:%S')

            cloud_config = {'secure_connect_bundle': secure_connect_bundle}

            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            lg.info("Deleting of record get started.....")

            session.execute(f"DELETE FROM {keyspace}.{table_name} WHERE {primary_column} = {primary_value};")

            lg.info(f"Record successfully get deleted from table {table_name} under keyspace {keyspace}")
            return f"Record successfully get deleted from table {table_name} under keyspace {keyspace}"


        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/bulk_insert', methods = ["POST"])

def bulk_insert():

    if (request.method == 'POST'):

        secure_connect_bundle = request.json['secure_connect_bundle']
        client_id = request.json['client_id']
        client_secret = request.json['client_secret']
        keyspace = request.json['keyspace']
        table_name = request.json['table_name']
        file_name = request.json['file_name']
        col0 = request.json['col0']
        col1 = request.json['col1']
        col2 = request.json['col2']
        col3 = request.json['col3']
        col4 = request.json['col4']


        try:

            lg.basicConfig(filename="Cassandra_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            cloud_config = {'secure_connect_bundle': secure_connect_bundle}

            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            lg.info("Bulk insertion of record get started.....")

            data = pd.read_csv(f"{file_name}")
            row_count = 0

            for i, row in data.iterrows():

                query = (f"insert into {keyspace}.{table_name} ({col0}, {col1}, {col2}, {col3}, {col4}) values (%s,%s,%s,%s,%s)")

                session.execute(query, tuple(row))
                lg.info("Record inserted !!!")

                row_count = row_count + 1


            lg.info(f"Bulk insertion successfully get completed from file {file_name}  to table {table_name} under keyspace {keyspace}")
            lg.info(f"Total {row_count} record get inserted !!")

            return f"Bulk insertion successfully get completed from file {file_name}  to table {table_name} under keyspace {keyspace}"


        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


@app.route('/download_data', methods = ["POST"])

def download_data():

    if (request.method == 'POST'):

        secure_connect_bundle = request.json['secure_connect_bundle']
        client_id = request.json['client_id']
        client_secret = request.json['client_secret']
        keyspace = request.json['keyspace']
        table_name = request.json['table_name']

        try:

            lg.basicConfig(filename="Cassandra_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            cloud_config = {'secure_connect_bundle': secure_connect_bundle}

            auth_provider = PlainTextAuthProvider(client_id, client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            lg.info("Downloading of data get started.....")

            no_of_records = 0
            main_list = []

            for i in session.execute(f"select * from {keyspace}.{table_name};"):
                main_list.append(i)
                no_of_records = no_of_records + 1

            df = pd.DataFrame(main_list)
            name = f"{table_name}_data.csv"
            df.to_csv(name, index = False)

            lg.info(f"Data successfully downloaded at {os.getcwd()} with name {name}")
            lg.info(f"Total number of records downloaded: {no_of_records}")
            lg.info("<<<<<<<TASK COMPLETED SUCCESSFULLY>>>>>>>")

            return f"Data successfully downloaded at {os.getcwd()} with name {name}"

        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occurred")
            lg.exception(str(e))


if __name__ == "__main__":
    app.run(debug=True)


