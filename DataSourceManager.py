from time import sleep, perf_counter
from threading import Thread
import sqlite3
import csv
from tracemalloc import start
from ConfusionMatrixManager import *

class DataSourceManager:
  
    table_name = "table3"
    filepath = "sample_data_tazi.csv"
    window_data = None

    def __init__(self, db_name, filepath=None):
        self.filepath = filepath
        self.db_name = db_name
        self.filepath = filepath if filepath is not None else "default"
      
    def pass_from_csv_to_db(self):
        chunksize = 20
        with pd.read_csv(self.filepath, chunksize=chunksize, header=None, skiprows = 1) as reader:
            for i, chunk in enumerate(reader):
                self.chunk_list.append(chunk)  
                chunk = list(chunk.itertuples(index=False, name=None))       
                iterator = map(lambda c: list(c), chunk)
                formatted_chunk= list(iterator)
                self.insert_data(formatted_chunk)
                # sleep(1)        

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
def populate_data_task(filepath, db_name):
    # Part 1: Continuous Data Source
    dsm = DataSourceManager(filepath, db_name) 
    dsm.db_connection() 
    ## uncomment below when the table is created from scratch
    data_source_table_name = "table3"
    # d1.create_data_table()
    # d1.pass_from_csv_to_db(data_source_table_name)
    dsm.display_table(data_source_table_name)

    ## uncomment below or deletion
    #d1.delete_data(conf_table_name)

def sliding_window_data_to_cmm(table_name, db_name):
    # Part 2: Calculating Confusion Matrix
    # this method sends data partitions to confusion matrix manager by sliding window approach

    # make filepath optional
    dsm = DataSourceManager(db_name) 
    ## uncomment below when the table is created from scratch
    conf_table_name = "conf_matrix4"
    #d1.create_conf_matrix_table(conf_table_name)

    dsm.sliding_window_data_to_cmm(conf_table_name)    
    cmm = ConfusionMatrixManager(3, ["A","B"])
    window_range = 10
    db_size = 20
    index = 0
    conf_matrix_array = []
    while index<db_size-window_range:
        start_index = index
        end_index = index + window_range
        query = "Select * from "+table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
        self.window_data = self.execute_and_get_query(query)
        cmm.table_divide_and_format(self.window_data)
        cmm.calculate_avg_preds()
        cmm.calculate_pred_list()
        conf_matrix = cmm.calculate_confusion_matrix()
        conf_matrix_array.append(cmm.calculate_confusion_matrix())
        # start confusion matrix saving
        self.save_confusion_matrix(table_name, conf_matrix)
        # end confusion matrix saving
        index += 1




# start_time = perf_counter()
# t1 = Thread(target=d1.populate_data_task)
# t2 = Thread(target=d1.sliding_window_data_to_cmm)

# t1.start()
# t2.start()

# t1.join()
# t2.join()

# end_time = perf_counter()

# print(f"It took {end_time-start_time: 0.2f} secs to complete.")
# d1.read_from_csv_and_insert_data()
# d1.database_connection_test1()
# print("--------------")
# d1.database_connection_test2()
##
##


