from time import sleep, perf_counter
from threading import Thread
import sqlite3
from tracemalloc import start
from ConfusionMatrixManager import *
import os
import pandas as pd
import schedule 


class DataSourceManager:
    data_path = "sample_data_tazi.csv"

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
                sleep(0.1)        

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
        print("fetch all select: ", cur.fetchall())
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

# main methods
def populate_data_task(dsm, data_table_name):
    # Part 1: Continuous Data Source

    print("Thread 1 - Populating data task!")
    # dsm = DataSourceManager(filepath, data_table_name, db_name) 
    dsm.db_connection() 
    
    ## uncomment below or deletion
    # dsm.delete_data(data_table_name)

    # dsm.pass_from_csv_to_db(data_table_name)
    # dsm.display_table(data_table_name)


def sliding_window_data_to_cmm(dsm, conf_mat_table_name, data_table_name):
    # Part 2: Calculating Confusion Matrix
    # this method sends data partitions to confusion matrix manager by sliding window approach
    print("Thread 2 - sliding_window task")
    cmm = ConfusionMatrixManager(3, ["A","B"])
    window_range = 10
    db_size = 100
    index = 0
    conf_matrix_array = []

    def fetch_sliding_data(dsm, db_size, window_range, index):
        start_time = perf_counter()
        # TODO: Add a condition to check if there is an enough instance in db
        query = "SELECT COUNT(*) FROM "+data_table_name
        cur_db_size = dsm.get_query_results(query)
        while index<db_size-window_range and index+window_range<cur_db_size:
            print("TEST 1 - cur_db_size: ", cur_db_size)
            print("TEST 2 - end_index: ", index+window_range)
            start_index = index
            end_index = index + window_range
            query = "Select * from "+data_table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
            dsm.window_data = dsm.get_query_results(query)
            cmm.table_divide_and_format(dsm.window_data)
            cmm.calculate_avg_preds()
            cmm.calculate_pred_list()
            conf_matrix = cmm.calculate_confusion_matrix()
            print("Thread 2 - confusion matrix: ", conf_matrix)
            conf_matrix_array.append(cmm.calculate_confusion_matrix())
            # start confusion matrix saving
            dsm.save_confusion_matrix(conf_mat_table_name, conf_matrix)
            # end confusion matrix saving execute_and_get_query
            index += 1

        end_time = perf_counter()
        print(f"sliding_window_data time: It secs.: ", end_time-start_time)

    schedule.every(1).seconds.do(fetch_sliding_data(cmm, window_range, db_size, index, conf_matrix_array))


if __name__ == "__main__":

    start_time = perf_counter()

    # a method is needed to init db

    filepath = "sample_data_tazi.csv"
    data_table_name = "table5"
    conf_mat_table_name = "conf_matrix5"
    db_name = "Tazi"

    dsm = DataSourceManager(conf_mat_table_name, db_name) 
    dsm.db_connection()
    # dsm.create_data_table(data_table_name)
    # dsm.create_conf_matrix_table(conf_mat_table_name)

    datasource_populating_thread = Thread(target=populate_data_task(dsm, data_table_name))
    confusion_matrix_calculating_thread = Thread(target=sliding_window_data_to_cmm(dsm, conf_mat_table_name, data_table_name))

    # datasource_populating_thread.start()
    # confusion_matrix_calculating_thread.start()

    # datasource_populating_thread.join()
    # confusion_matrix_calculating_thread.join()

    # end_time = perf_counter()
    # print(f"It took {end_time-start_time: 0.2f} secs to complete.")
