from locale import currency
from multiprocessing.connection import wait
from time import sleep, perf_counter
import sqlite3
from tracemalloc import start
from ConfusionMatrixManager import *
import os
import pandas as pd
from threading import Thread


class DataSourceManager:
    data_path = "sample_data_tazi.csv"
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
                # TODO: Modify sleep
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
        res = cur.fetchall()
        con.commit()
        con.close()

    def get_query_results(self, query, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute(query)
        res = cur.fetchall()
        con.commit()
        con.close()
        return res
    
    def get_table_len(self, table_name):
        query = "SELECT * FROM "+table_name
        return len(self.get_query_results(query, table_name))

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

    # test purposes: delete
    def test_create_data_table(self, table_name):
        query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        res = self.get_query_results(query, table_name) 
        x = [i[0] for i in res]
        print("table list: ", x)
        print("table_name: ", table_name)
        if table_name in x:
            print("true")
        else:
            print("false")

# main methods
def populate_data_task(dsm, data_table_name):
    # Part 1: Continuous Data Source
    print("Thread 1 - Populating data task!")
    dsm.db_connection() 
    
    ## uncomment below or deletion
    dsm.delete_data(data_table_name)

    # dsm.pass_from_csv_to_db(data_table_name)
    dsm.display_table(data_table_name)


def sliding_window_data_to_cmm(dsm, conf_mat_table_name, data_table_name):
    # Part 2: Calculating Confusion Matrix
    # this method sends data partitions to confusion matrix manager by sliding window approach
    print("Thread 2 - Sliding Window Task")
    cmm = ConfusionMatrixManager(3, ["A","B"])
    window_range = 10
    db_size_limit = 100
    index = 0
    conf_matrix_array = []

    # TODO: Add a condition to check if there is an enough instance in db
    query = "SELECT COUNT(*) FROM "+data_table_name
    reading_finished = False
    wait_count = 0

    while not reading_finished:
        cur_table_size = dsm.get_table_len(dsm, data_table_name)
        print("Table size - :", cur_table_size)
        if cur_table_size == 0:
            print("INFO - Thread 2 - DB is empty!")
    
        # check if it is end of the db
        if index+window_range == db_size_limit - 1:
            reading_finished = True
            print("INFO - Thread 2 - Reading data is finished!")

        # check if data available for reading in db
        if  index + window_range < cur_table_size:
            print("\nINFO - ...READING...\n")
            wait_count = 0  
            start_index = index
            end_index = index + window_range
            query = "Select * from "+data_table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
            dsm.window_data = dsm.get_query_results(query, data_table_name)
            cmm.table_divide_and_format(dsm.window_data)
            cmm.calculate_avg_preds()
            cmm.calculate_pred_list()
            conf_matrix = cmm.calculate_confusion_matrix()
            conf_matrix_array.append(cmm.calculate_confusion_matrix())
            dsm.save_confusion_matrix(conf_mat_table_name, conf_matrix)
            index += 1
        else:
        # if not available data then wait for datasource to populate db

            if wait_count < 50000:
                print("\nINFO - ...WAITING...")
                print("INFO - Thread 2 - Current db size is: ", cur_table_size)
                print("INFO - Wait count: ", wait_count)
                wait_count += 1  
            else:
                print("INFO - Thread 2 - Waited too long, so exiting the system.\n")
                reading_finished = True

        print("INFO - Thread 2 - Current start_index: ", index)
        print("INFO - Thread 2 - Current end_index: ", index+window_range)

    # TODO: Implement it later
    #schedule.every(1).seconds.do(lambda:fetch_sliding_data,dsm, window_range, db_size, index, conf_matrix_array)
    #schedule.every(1).seconds.do(dummy_func)

#if __name__ == "__main__":

    # # a method is needed to init db
    # filepath = "sample_data_tazi.csv"
    # data_table_name = "table11"
    # conf_mat_table_name = "conf_matrix11"
    # db_name = "Tazi"

    # dsm = DataSourceManager(conf_mat_table_name, db_name) 
    # dsm.db_connection()
    # # dsm.create_data_table(data_table_name)
    # # dsm.create_conf_matrix_table(conf_mat_table_name)
    
    # datasource_populating_thread = Thread(target=populate_data_task, args=(dsm, data_table_name))
    # confusion_matrix_calculating_thread = Thread(target=sliding_window_data_to_cmm, args=(dsm, conf_mat_table_name, data_table_name))

    # datasource_populating_thread.start()
    # confusion_matrix_calculating_thread.start()
   
    # datasource_populating_thread.join()
    # confusion_matrix_calculating_thread.join()
