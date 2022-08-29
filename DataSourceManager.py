import pandas as pd
from time import sleep, perf_counter
from threading import Thread
import sqlite3
import csv
import numpy as np

class DataSourceManager:
    chunk_list = []
    con = None
    table_name = "table1"
    con = None

    def __init__(self, filepath, db_name):
        self.filepath = filepath
        self.db_name = db_name

    def read_from_csv(self):
        chunksize = 5
        with pd.read_csv(self.filepath, chunksize=chunksize, header=None, skiprows = 1) as reader:
            for i, chunk in enumerate(reader):
                # print("%d th chunk" %i)
                print("-----")
                # print("Chunk is: %s" %chunk)
                self.chunk_list.append(chunk)
                # sleep(1)

    def database_connection(self):
        global chunk
        
        try:
            # Connect to DB and create a cursor
            self.con = sqlite3.connect(self.db_name+".db")
            cursor = self.con.cursor()
        
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
        #     if self.con:
        #         self.con.close()
        #         print('SQLite Connection closed')

    def populate_data_task(self):
        print("Start populating data...")
        self.read_from_csv()

    def insert_data(self):
        cur = self.con.cursor()
        a_file = open("sample_data_tazi.csv")
        rows = csv.reader(a_file)
        next(rows)
        print("Rows: ", rows)
        print("Rows-next: ", rows)

        cur.executemany("INSERT INTO "+self.table_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        cur.execute("SELECT * FROM "+self.table_name)
        print("FETCH - insert_data: ", cur.fetchall())

        # query = "DELETE FROM "+db_name
        # cur.execute(query)
        # print("DELETION check: ", cur.rowcount)
        # cur.execute("SELECT * FROM "+db_name)
        # print("fetch all2 select after deletion: ", cur.fetchall())

        self.con.commit()
        self.con.close()

    def create_table(self, table_name):
        self.table_name = table_name
        cur = self.con.cursor()
        cur.execute("CREATE TABLE "+table_name+"(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        print("FExable: ", cur.fetchone())
        self.con.commit()
        self.con.close()

    def delete_data(self, cursor):
        query = "DELETE FROM "+self.db_name
        cursor.execute(query)
        print("DELETION check: ", cursor.rowcount)     
        self.con.commit()
        self.con.close()
        

    def display_table(self):
        cur = self.con.cursor()
        cur.execute("SELECT * FROM "+self.db_name)
        print("fetch all select: ", cur.fetchall())
        self.con.commit()
        self.con.close()
        

    def execute_query(self, query):
        # add query argument
        cur = self.con.cursor()
        cur.execute(query)
        print("FETCH - execute_query: ", cur.fetchall())
        self.con.commit()
        self.con.close()

    def get_data_as_sliding_windows(self):
        start_index = 1
        end_index = 1000
        query = "Select * from "+d1.table_name+" where id between "+str(start_index)+" and "+str(end_index)
        test_query = "Select * from "+self.table_name
        test_query = "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
        self.read_data(test_query)
        cur = self.con.cursor()
        cur.execute(test_query)
        print("fetch all select: ", cur.fetchall())

d1 = DataSourceManager("sample_data_tazi.csv", "Tazi") 
d1.database_connection() 
#d1.create_table("table1")
# query0 = "SELECT name FROM sqlite_schema WHERE type ='table' AND name NOT LIKE 'sqlite_%';"
start_index, end_index = 1,5
query = "Select * from "+d1.table_name+" where id between "+str(start_index)+" and "+str(end_index)


query = "Select * from "+d1.table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)

d1.execute_query(query)
# d1.insert_data()
# d1.display_table()




















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


