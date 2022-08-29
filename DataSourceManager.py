from time import sleep, perf_counter
from threading import Thread
import sqlite3
import csv
from tracemalloc import start
from ConfusionMatrixManager import *

class DataSourceManager:
    chunk_list = []
    con = None
    table_name = "table3"
    con = None
    window_data = None

    def __init__(self, filepath, db_name):
        self.filepath = filepath
        self.db_name = db_name
      
    def read_from_csv_and_insert_data(self):
        chunksize = 20
        with pd.read_csv(self.filepath, chunksize=chunksize, header=None, skiprows = 1) as reader:
            for i, chunk in enumerate(reader):
                # print("%d th chunk" %i)
                print("-----")
                # print("Chunk is: %s" %chunk)
                self.chunk_list.append(chunk)  
                chunk = list(chunk.itertuples(index=False, name=None))       
                iterator = map(lambda c: list(c), chunk)
                chunk = list(iterator)
                self.insert_data(chunk)
                # test 
                # sleep(1)        

    def database_connection(self):
        global chunk

        try:
            # Connect to DB and create a cursor
            con = sqlite3.connect(self.db_name+".db")
            cursor = con.cursor()
        
            # Write a query and execute it with cursor
            query = 'select sqlite_version();'
            cursor.execute(query)
        
            # Fetch and output result
            result = cursor.fetchall()
            print('SQLite Version is {}'.format(result))
        
            # Close the cursor
            cursor.close()
        
        # Handle errors
        except sqlite3.Error as error:
            print('Error occured - ', error)
        # Close DB Connection irrespective of success
        # or failure

        # TODO: Close db connection after exiting the app
        # finally:
        #     if con:
        #         con.close()
        #         print('SQLite Connection closed')

    def insert_data(self, chunk, table_name):
        #print("chunk: ", chunk)
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        query = "INSERT INTO "+table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        cur.executemany(query, chunk)
        print("FETCH - insert_data: ", cur.fetchall())
        con.commit()
        con.close()

    def insert_confusion_matrix(self, table_name, confusion_matrix_str):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("INSERT INTO "+table_name+" (conf_mat_str) VALUES ( '"+confusion_matrix_str+"')")
        con.commit()

    def create_data_table(self, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("CREATE TABLE "+table_name+"(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        print("FETCH: ", cur.fetchone())
        con.commit()
        con.close()

    def create_conf_matrix_table(self, table_name):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("CREATE TABLE "+table_name+"(conf_mat_str)")
        print("FETCH - confusion matrix: ", cur.fetchone())
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

    def execute_and_get_query(self, query):
        # add query argument
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute(query)
        fetched_res = cur.fetchall()
        #print("FETCH - execute_query: ", self.window_data)
        con.commit()
        con.close()
        return fetched_res
    
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
        query = "Select * from "+table_name
        conf_mat_data = self.execute_and_get_query(query)
        index = 0
        for conf_tup in conf_mat_data:
            conf_mat_data  = conf_tup[0].split(" ")
            conf_mat_data = np.array(conf_mat_data).reshape(2,2)
            index += 1

        
## A sample database method
d1 = DataSourceManager("sample_data_tazi.csv", "Tazi") 
d1.database_connection() 
#d1.create_data_table("table3")
# d1.read_from_csv_and_insert_data()
# d1.delete_data()
# d1.display_table()

## do it for once
#d1.create_conf_matrix_table(table_name_2)

conf_table_name = "conf_matrix4"

## transfer data to cmm
cmm = ConfusionMatrixManager(3, ["A","B"])
window_range = 10
db_size = 20
index = 0

d1.delete_data(conf_table_name)

table_name = "table3"
conf_matrix_array = []
while index<db_size-window_range:
    start_index = index
    end_index = index + window_range
    #print("index pairs: ", (start_index, end_index))
    query = "Select * from "+table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
    d1.window_data = d1.execute_and_get_query(query)
    cmm.table_divide_and_format(d1.window_data)
    cmm.calculate_avg_preds()
    cmm.calculate_pred_list()
    conf_matrix = cmm.calculate_confusion_matrix()
    conf_matrix_array.append(cmm.calculate_confusion_matrix())
    # start confusion matrix saving
    d1.save_confusion_matrix(conf_table_name, conf_matrix)
    # end confusion matrix saving
    index += 1
 
d1.display_table(conf_table_name)
d1.fetch_confusion_matrix(conf_table_name)
  

















#d1.read_data()


#d1.create_data_table(con, table_name)
#d1.display_table()
#d1.get_data_as_sliding_windows()




# start_time = perf_counter()
# t1 = Thread(target=d1.populate_data_task)
# t2 = Thread(target=d1.calculation_data_task)

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


