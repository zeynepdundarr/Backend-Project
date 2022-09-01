from time import sleep
import sqlite3
from ConfusionMatrixManager import *
import os
import pandas as pd

class DataSourceManager:

    def __init__(self, db_name, data_table_name, conf_mat_table_name, filepath, db_size_limit):
        self.db_name = db_name      
        self.data_table_name = data_table_name 
        self.conf_mat_table_name = conf_mat_table_name
        self.filepath = filepath 
        self.db_size_limit = db_size_limit
   
    def pass_from_csv_to_db(self):
        # chunksize = 500 with real values
        chunksize = 20
        with pd.read_csv(self.filepath, chunksize=chunksize, header=None, skiprows = 1) as reader:
            for i, chunk in enumerate(reader):
                print("Thread 1 - Chunk: ", i)
                chunk = list(chunk.itertuples(index=False, name=None))       
                iterator = map(lambda c: list(c), chunk)
                formatted_chunk= list(iterator)
                self.insert_data(formatted_chunk)
                sleep(1)        

    def db_connection(self):
        global chunk
        try:
            con = sqlite3.connect(self.db_name+".db")
            cursor = con.cursor()
            query = 'select sqlite_version();'
            cursor.execute(query)
            result = cursor.fetchall()
            print('SQLite Version is {}'.format(result))
            cursor.close()
        except sqlite3.Error as error:
            print('Error occured - ', error)
        finally:
            if con:
                con.close()
                print('SQLite Connection closed')
    
    def insert_data(self, chunk):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        query = "INSERT INTO "+self.data_table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        cur.executemany(query, chunk)
        con.commit()
        con.close()

    def insert_confusion_matrix(self, confusion_matrix_str):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        query = "INSERT INTO "+self.conf_mat_table_name+" (conf_mat_str) VALUES ('"+confusion_matrix_str+"')"

        cur.execute(query)
        con.commit()

    def create_data_table(self):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("CREATE TABLE "+self.data_table_name+"(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        con.commit()
        con.close()

    def create_conf_matrix_table(self):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("CREATE TABLE "+self.conf_mat_table_name+"(conf_mat_str)")
        con.commit()
        con.close()

    def delete_data(self, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cursor = con.cursor()
        query = "DELETE FROM "+table_name
        cursor.execute(query)
        print("DELETION check: ", cursor.rowcount)     
        con.commit()
        con.close()
        
    def display_table_contents(self, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("SELECT * FROM "+table_name)
        res = cur.fetchall()
        print("INFO - table contents are displayed:", res)
        con.commit()
        con.close()

    def display_tables_in_db(self):
        query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        res = self.get_query_results(query)
        x = [i[0] for i in res]
        print("INFO - Tables in db: ", x)
        return x
        
    def get_query_results(self, query):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute(query)
        res = cur.fetchall()
        con.commit()
        con.close()
        return res
    
    def get_table_len(self, table_name):
        query = "SELECT * FROM "+table_name
        return len(self.get_query_results(query))

    def save_confusion_matrix(self, confusion_matrix):
        confusion_matrix_str = ""
        first = True
        for row in confusion_matrix:
            for val in row:
                if first:
                    confusion_matrix_str+=str(val)
                    first = False
                else:
                    confusion_matrix_str+=" "+str(val)
        self.insert_confusion_matrix(confusion_matrix_str)

    def fetch_confusion_matrix(self):
        query = "SELECT * FROM "+self.conf_mat_table_name
        conf_mat_data = self.get_query_results(query)
        index = 0
        for conf_tup in conf_mat_data:
            conf_mat_data  = conf_tup[0].split(" ")
            conf_mat_data = np.array(conf_mat_data).reshape(2,2)
            index += 1
