
from flask import Flask, request, jsonify,render_template
import logging as lg
import pandas as pd
import mysql.connector as connection
import os

app = Flask(__name__)

@app.route('/create_connection', methods = ["POST"])

def create_connection():

    if (request.method == 'POST'):
        HOST = request.json['host']
        USER = request.json['user']
        PASSWORD = request.json['password']

        try:

            lg.basicConfig(filename="SQL_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')

            lg.info(f"Host id {HOST}, User name {USER} has tried to make a connection !!")

            mydb = connection.connect(host=HOST, user=USER, passwd=PASSWORD, use_pure=True)

            lg.info("Connection successfull !!")

            # check if the connection is established

            query = "SHOW DATABASES"

            cursor = mydb.cursor()  # create a cursor to execute queries
            cursor.execute(query)

            lg.info("query get executed successfully !!! !!")

            return jsonify(cursor.fetchall())

        except Exception as e:
            print("Check your log to know why your code is not working !!")
            lg.error("error has occured")
            lg.exception(str(e))


@app.route('/create_table', methods = ["POST"])

def create_table():

    if (request.method == 'POST'):

        HOST = request.json['host']
        USER = request.json['user']
        PASSWORD = request.json['password']

        Database = request.json['database']
        Table_Name = request.json['table']

        ColumnOne = request.json['columnone']
        ColumnOneDtype = request.json['columnonedtype']

        ColumnTwo = request.json['columntwo']
        ColumnTwoDtype = request.json['columntwodtype']

        ColumnThree = request.json['columnthree']
        ColumnThreeDtype = request.json['columnthreedtype']

        ColumnFour = request.json['columnfour']
        ColumnFourDtype = request.json['columnfourdtype']


        try:

            lg.basicConfig(filename="SQL_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            mydb = connection.connect(host=HOST, user=USER, passwd=PASSWORD, use_pure=True)


            cur = mydb.cursor()

            query = f"DROP TABLE IF  EXISTS {Database}.{Table_Name}"

            cur.execute(query)

            query = f"CREATE TABLE {Database}.{Table_Name} ({ColumnOne} {ColumnOneDtype},{ColumnTwo} {ColumnTwoDtype}, {ColumnThree} {ColumnThreeDtype}, {ColumnFour} {ColumnFourDtype})"

            cur.execute(query)

            lg.info(f"Table with name {Table_Name} under {Database} database get successfully created !!")

            cur = mydb.cursor()
            cur.execute(f"USE {Database} ")
            cur = mydb.cursor()
            cur.execute("SHOW TABLES")

            return jsonify(cur.fetchall())



        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occured")
            lg.exception(str(e))



@app.route('/insert_data', methods = ["POST"])

def insert_data():

    if (request.method == 'POST'):

        HOST = request.json['host']
        USER = request.json['user']
        PASSWORD = request.json['password']

        Database = request.json['database']
        Table_Name = request.json['table']
        ColumnOne = request.json['columnone']
        ColumnTwo = request.json['columntwo']
        ColumnThree = request.json['columnthree']
        ColumnFour = request.json['columnfour']

        try:

            lg.basicConfig(filename="SQL_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            mydb = connection.connect(host=HOST, user=USER, passwd=PASSWORD, use_pure=True)


            cur = mydb.cursor()

            query = f"INSERT INTO {Database}.{Table_Name} VALUES ({ColumnOne}, {ColumnTwo}, {ColumnThree}, {ColumnFour})"

            cur.execute(query)

            mydb.commit()

            lg.info("One record inserted !!")

            cur = mydb.cursor()

            query = f"SELECT * FROM {Database}.{Table_Name}"

            cur.execute(query)

            return jsonify(cur.fetchall())


        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occured")
            lg.exception(str(e))



@app.route('/update_record', methods = ["POST"])

def update_record():

    if (request.method == 'POST'):

        HOST = request.json['host']
        USER = request.json['user']
        PASSWORD = request.json['password']

        Database = request.json['database']
        Table_Name = request.json['table']
        UpdatingColumn = request.json['updatingcolumn']
        UpdateValue = request.json['updatevalue']
        ConditionColumn = request.json['conditioncolumn']
        ConditionValue = request.json['conditionvalue']


        try:

            lg.basicConfig(filename="SQL_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            mydb = connection.connect(host=HOST, user=USER, passwd=PASSWORD, use_pure=True)


            cur = mydb.cursor()

            query = f"UPDATE {Database}.{Table_Name} SET {UpdatingColumn} = {UpdateValue} WHERE {ConditionColumn} = {ConditionValue}"

            cur.execute(query)

            mydb.commit()

            lg.info("One record updated !!")

            cur = mydb.cursor()

            query = f"SELECT * FROM {Database}.{Table_Name} WHERE {UpdatingColumn} = {UpdateValue}"

            cur.execute(query)

            return jsonify(cur.fetchall())


        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occured")
            lg.exception(str(e))


@app.route('/delete_record', methods = ["POST"])

def delete_record():

    if (request.method == 'POST'):

        HOST = request.json['host']
        USER = request.json['user']
        PASSWORD = request.json['password']

        Database = request.json['database']
        Table_Name = request.json['table']
        Column = request.json['column']
        Value = request.json['value']



        try:

            lg.basicConfig(filename="SQL_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            mydb = connection.connect(host=HOST, user=USER, passwd=PASSWORD, use_pure=True)


            cur = mydb.cursor()

            query = f"DELETE FROM {Database}.{Table_Name} WHERE {Column} = {Value}"

            cur.execute(query)

            mydb.commit()

            lg.info("One record deleted !!")

            cur = mydb.cursor()

            query = f"SELECT * FROM {Database}.{Table_Name}"

            cur.execute(query)

            return jsonify(cur.fetchall())


        except Exception as e:

            print("Check your log to know why your code is not working !!")
            lg.error("error has occured")
            lg.exception(str(e))


@app.route('/bulk_insert', methods = ["POST"])

def bulk_insert():

    if (request.method == 'POST'):

        HOST = request.json['host']
        USER = request.json['user']
        PASSWORD = request.json['password']

        Database = request.json['database']
        Table_Name = request.json['table']
        FileName = request.json['filename']

        try:

            lg.basicConfig(filename="SQL_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            data = pd.read_csv(f"{FileName}")

            mydb = connection.connect(host=HOST, user=USER, password=PASSWORD)
            if mydb.is_connected():
                cursor = mydb.cursor()
                lg.info("Bulk insertion started.....")
                row_count = 0

                for i, row in data.iterrows():

                    sql = f"INSERT INTO {Database}.{Table_Name} VALUES (%s,%s,%s,%s)"
                    cursor.execute(sql, tuple(row))
                    lg.info("Record inserted !!!")
                    # the connection is not autocommitted by default, so we must commit to save our changes
                    mydb.commit()
                    row_count = row_count+1

                lg.info(f"Bulk insertion from {FileName} located at {os.getcwd()} is successfully completed !!")
                lg.info(f"Total {row_count} record get inserted !!")
                cur = mydb.cursor()

                query = f"SELECT * FROM {Database}.{Table_Name}"

                cur.execute(query)

                return jsonify(cur.fetchall())


        except Exception as e:
            print("you can check your log for more info if your code will fail")
            lg.error("error has occured")
            lg.exception(str(e))

        else:
            print("all data inserted")


@app.route('/download_data', methods = ["POST"])

def download_data():

    if (request.method == 'POST'):

        HOST = request.json['host']
        USER = request.json['user']
        PASSWORD = request.json['password']

        Database = request.json['database']
        Table_Name = request.json['table']

        try:

            lg.basicConfig(filename="SQL_log_file.log", level=lg.INFO, format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

            mydb = connection.connect(host=HOST, user=USER, password=PASSWORD)

            if mydb.is_connected():
                cursor = mydb.cursor()
                cursor.execute(f"select * from {Database}.{Table_Name}")
                main_list = []  #main list to append data in it
                for i in cursor.fetchall():
                    main_list.append(i)
                lg.info("Downloading in progress.......")

                lg.info(f"Fetching records from {Database}.{Table_Name}.....")

                lg.info("Records get appended into main_list")

                mydata = pd.DataFrame(main_list)  #converting main_list to DataFrame

                #saving DataFrame to csv file

                name = f"{Table_Name}_data.csv"
                mydata.to_csv(name, index = False)

                lg.info(f"File successfully downloaded at {os.getcwd()} with name {name}")

                return f"File successfully downloaded at {os.getcwd()} with name {name}"

        except Exception as e:
            print("you can check your log for more info if your code will fail")
            lg.error("error has occured")
            lg.exception(str(e))

        else:
            print(f"File Downloaded at {os.getcwd()}")




if __name__ == "__main__":
    app.run(debug=True)







