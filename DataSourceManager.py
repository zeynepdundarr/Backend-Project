from time import sleep, perf_counter
from threading import Thread
import sqlite3
import csv
from tracemalloc import start
from ConfusionMatrixManager import *

class DataSourceManager:
    chunk_list = []
    con = None
    table_name = "table1"
    con = None
    cur_data = None

    def __init__(self, filepath, db_name):
        self.filepath = filepath
        self.db_name = db_name
      
    def read_from_csv2(self):
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
                break
                # sleep(1)

    def read_from_csv(self):
        a_file = open("sample_data_tazi.csv")
        rows = csv.reader(a_file)
        next(rows)
        next(rows)

        for i in rows:
            print("TEST 8 - Is starting from next?: ", next(i))
        

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
        self.read_from_csv()

    def insert_data(self, chunk):
        #print("chunk: ", chunk)
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        a_file = open("sample_data_tazi.csv")
        rows = csv.reader(a_file)
        next(rows)
        # print("TEST 2 - Rows: ", rows)
        # print("TEST 2 - chunk: ", chunk)
        cur.executemany("INSERT INTO "+self.table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)", chunk)
        #cur.execute("SELECT * FROM "+self.table_name)
        print("FETCH - insert_data: ", cur.fetchall())

        con.commit()
        con.close()
    
    def insert_data2(self, chunk):
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
    
        cur.executemany("INSERT INTO "+self.table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)", chunk)
        print("TEST 1 - FETCH - insert_data: ", cur.fetchall())
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

    def execute_query(self, query):
        # add query argument
        con = sqlite3.connect(self.db_name+".db")
        cur = con.cursor()
        cur.execute(query)
        res = cur.fetchall()
        print("FETCH - execute_query: ", res)
        con.commit()
        con.close()
        return res

    def get_data_as_sliding_windows(self, query):
        start_index = 1
        end_index = 1000
        res = self.execute_query(query)
        print("fetch all select: ", res)
        return res

## A sample database method
d1 = DataSourceManager("sample_data_tazi.csv", "Tazi") 
d1.database_connection() 
#d1.create_table("table3")
d1.read_from_csv2()
#d1.delete_data()
d1.display_table()

# ## displaying tables in db
# query0 = "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"

# query = "Select * from "+d1.table_name+" where id between "+str(start_index)+" and "+str(end_index)
# query = "Select * from "+d1.table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
# d1.execute_query(query)

# d1.insert_data()
# d1.display_table()


# transfer data to cmm
# cmm = ConfusionMatrixManager(d1.cur_data, 3, ["A","B"])
# window_range = 10
# db_size = 100
# index = 0
# while index<db_size-window_range:
#     start_index = index
#     end_index = index + window_range
#     print("index pairs: ", (start_index, end_index))
#     # query = "Select * from "+d1.table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
#     # sliding_window_data = d1.get_data_as_sliding_windows(query)
#     # d1.cur_data = sliding_window_data
#     # print("Sliding window data: ", sliding_window_data)
#     # cmm.table_divide_and_format()

#     # update indexes
#     index += 1
    

















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
# d1.read_from_csv()
# d1.database_connection_test1()
# print("--------------")
# d1.database_connection_test2()
##
##


