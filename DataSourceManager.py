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

    def populate_data_task(self):
        print("Start populating data...")
        self.read_from_csv_and_insert_data()

    def insert_data(self, chunk):
        #print("chunk: ", chunk)
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.executemany("INSERT INTO "+self.table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)", chunk)
        print("FETCH - insert_data: ", cur.fetchall())
        con.commit()
        con.close()
    
    def insert_data2(self, chunk):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.executemany("INSERT INTO "+self.table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)", chunk)
        con.commit()
        con.close()

    def create_table(self, table_name):
        self.table_name = table_name
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("CREATE TABLE "+table_name+"(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        print("FExable: ", cur.fetchone())
        con.commit()
        con.close()

    def delete_data(self):
        con = sqlite3.connect(self.db_name+".db")
        cursor = con.cursor()
        query = "DELETE FROM "+self.table_name
        cursor.execute(query)
        print("DELETION check: ", cursor.rowcount)     
        con.commit()
        con.close()
        
    def display_table(self):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute("SELECT * FROM "+self.table_name)
        print("fetch all select: ", cur.fetchall())
        con.commit()
        con.close()

    def execute_and_get_query(self, query):
        # add query argument
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute(query)
        self.window_data = cur.fetchall()
        #print("FETCH - execute_query: ", self.window_data)
        con.commit()
        con.close()
        return self.window_data
        

## A sample database method
d1 = DataSourceManager("sample_data_tazi.csv", "Tazi") 
d1.database_connection() 
#d1.create_table("table3")
# d1.read_from_csv_and_insert_data()
#d1.delete_data()
# d1.display_table()

# transfer data to cmm
cmm = ConfusionMatrixManager(3, ["A","B"])
window_range = 10
db_size = 20
index = 0

conf_matrix_array = []
while index<db_size-window_range:
    start_index = index
    end_index = index + window_range
    #print("index pairs: ", (start_index, end_index))
    query = "Select * from "+d1.table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
    d1.window_data = d1.execute_and_get_query(query)
    cmm.table_divide_and_format(d1.window_data)
    cmm.calculate_avg_preds()
    cmm.calculate_pred_list()
    conf_matrix = cmm.calculate_confusion_matrix()
    conf_matrix_array.append(cmm.calculate_confusion_matrix())
    print("TEST 15: ", conf_matrix)
    # update indexes
    index += 1
    print("TEST 16 - index: ", index)

print("TEST 1: Confusion matrix array: ", conf_matrix_array)
  

















#d1.read_data()


#d1.create_table(con, d1.table_name)
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


