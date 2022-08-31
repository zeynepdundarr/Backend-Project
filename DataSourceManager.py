from time import sleep
import sqlite3
from ConfusionMatrixManager import *
import os
import pandas as pd

class DataSourceManager:

    data_path = "sample_data.csv"
    ## ful data path is below
    # data_path = "tazi-se-interview-project-data.csv"

    def __init__(self, db_name, data_table_name = None, conf_mat_table_name = None, filepath=None):
        self.db_name = db_name      
        # TODO: try with filepath = None
        print("DataSourceManager has been initialized!")
        data_table_name = data_table_name if data_table_name is not None else "default-datasource"
        conf_mat_table_name = conf_mat_table_name if conf_mat_table_name is not None else "default-confusion-matrix"
        self.filepath = filepath if filepath is not None else os.getcwd()+"/"+self.data_path
      
    def pass_from_csv_to_db(self, table_name):
        chunksize = 20
        with pd.read_csv(self.filepath, chunksize=chunksize, header=None, skiprows = 1) as reader:
            for i, chunk in enumerate(reader):
                print("Thread 1 - Chunk: ", i)
                chunk = list(chunk.itertuples(index=False, name=None))       
                iterator = map(lambda c: list(c), chunk)
                formatted_chunk= list(iterator)
                self.insert_data(formatted_chunk, table_name)
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
    
    def insert_data(self, chunk, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        query = "INSERT INTO "+table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        cur.executemany(query, chunk)
        con.commit()
        con.close()

    def insert_confusion_matrix(self, table_name, confusion_matrix_str):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("INSERT INTO "+table_name+" (conf_mat_str) VALUES ('"+confusion_matrix_str+"')")
        con.commit()

    def create_data_table(self, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("CREATE TABLE "+table_name+"(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        con.commit()
        con.close()

    def create_conf_matrix_table(self, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("CREATE TABLE "+table_name+"(conf_mat_str)")
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
        
    def display_table(self,table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("SELECT * FROM "+table_name)
        #cur.execute("SELECT * FROM table12")
        res = cur.fetchall()
        con.commit()
        con.close()

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
        #print("Test 200 - Table len is: ", len(self.get_query_results(query, table_name)))
        return len(self.get_query_results(query))

    def save_confusion_matrix(self, table_name, confusion_matrix):
        confusion_matrix_str = ""
        first = True
        for row in confusion_matrix:
            for val in row:
                if first:
                    confusion_matrix_str+=str(val)
                    first = False
                else:
                    confusion_matrix_str+=" "+str(val)
        self.insert_confusion_matrix(table_name, confusion_matrix_str)
        self.display_table(table_name)

    def fetch_confusion_matrix(self, table_name):
        query = "SELECT * FROM "+table_name
        conf_mat_data = self.get_query_results(query)
        index = 0
        for conf_tup in conf_mat_data:
            conf_mat_data  = conf_tup[0].split(" ")
            conf_mat_data = np.array(conf_mat_data).reshape(2,2)
            index += 1
