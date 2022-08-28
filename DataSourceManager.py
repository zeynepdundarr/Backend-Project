
import pandas as pd
from time import sleep, perf_counter
from threading import Thread
import sqlite3
import csv
import numpy as np

class ConfusionMatrixManager:

    given_label_list = None
    prob_values = None
    avg_probs_a = None
    avg_probs_b = None
    pred_label_list = None

    def __init__(self, table, model_no, labels):
        self.table = table
        self.model_no = model_no
        self.labels = labels
        print("Model initialized!")

    def table_divide_and_format(self):
        # TODO: can it be more optimized that row by row np.array conversion
        # info: divides table into actual labels list and probility values
        x = [row[1] for row in self.table]
        self.given_label_list = np.array(list([row[1] for row in self.table]))
        print("TEST 3 - x: ", np.array(x))
        print("TEST 3 - cmm.given: ", cmm.given_label_list)
        self.prob_values = [np.array(list(map(float,row[2:len(row)])), dtype = np.float32) for row in self.table]


    def get_avg_preds(self):
        # TODO: make it scalable for n models
        np_table = np.array(self.prob_values)
        w1, w2, w3 = 1,1,1
        #0.5, 0.6, 0.7
        weights_label_a = np.array([w1, 0, w2, 0, w3, 0])
        weights_label_b = np.array([0, w1, 0, w2, 0, w3])

        self.avg_probs_a = np.sum(np_table*weights_label_a, axis=1)/3
        self.avg_probs_b = np.sum(np_table*weights_label_b, axis=1)/3
     
    def confusion_matrix(self):
        actual = self.given_label_list
        preds = self.pred_label_list

        labels = np.unique(actual)
        matrix = np.zeros((len(labels), len(labels)))
        print("Labels: ",labels)
        print("actual: ", actual)
        print("preds: ", preds)
        for i in range(len(labels)):
            for j in range(len(labels)):
                matrix[i,j] = np.sum( (actual==labels[i]) & (preds==labels[j]))
        return matrix

    def get_pred_list(self):
        res = self.avg_probs_a - self.avg_probs_b
        self.pred_label_list = np.array(["A" if x>=0 else "B" for x in res])

# Run ConfusionMatrixManager
sample_table = [('2', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
                ('3', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
                ('4', 'B', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
                ('5', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0'),
                ('6', 'A', '0.1', '0.9', '0.2', '0.8', '1.0', '0')]

cmm = ConfusionMatrixManager(sample_table, 3, ["A", "B"])
cmm.table_divide_and_format()
cmm.get_avg_preds()
cmm.get_pred_list()


class DataSourceManager:
    chunk_list = []

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
    
    def populate_data_task(self):
        print("Start populating data...")
        self.read_from_csv()

    def calculation_data_task(self):
        print("Start calculating data...")
        sleep(5)

    def database_connection2(self):
        global chunk
        conn = sqlite3.connect('sql.db')
        cur = conn.cursor()
        ## table creation
        # cur.execute("CREATE TABLE DATASOURCE(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        res = cur.execute("SELECT name FROM sqlite_master")


        print("Check if chunklist is full: ", self.chunk_list)
        # res = conn.executemany("insert into DataSource(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B) values (?,?,?,?,?,?,?,?)", self.chunk_list)
        res = conn.execute("insert into DataSource(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B) values (?,?,?,?,?,?,?,?)", self.chunk_list)

        res = conn.execute("SELECT COUNT(*) FROM DataSource")
        print("Res: ", res.fetchone())
        print("Res rowcount: ", res.rowcount)
        # res = conn.execute("select * from DataSource")


    def database_connection(self):
        global chunk

        try:
            # Connect to DB and create a cursor
            sqliteConnection = sqlite3.connect('sql.db')
            cursor = sqliteConnection.cursor()
            print('DB Init')
        
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
        finally:
            if sqliteConnection:
                sqliteConnection.close()
                print('SQLite Connection closed')
        
                
    def database_connection_test1(self):
        con = sqlite3.connect("data.db")
        cur = con.cursor()

        #cur.execute("CREATE TABLE data(col1,col2)")
        a_file = open("test.csv")
        rows = csv.reader(a_file)
        print("rows test1: ", rows)
        #cur.executemany("INSERT INTO data VALUES (?, ?)", rows)
        cur.execute("SELECT * FROM data")
        print("fetch all1 select: ",cur.fetchall())
        con.commit()
        con.close()

    
    def database_connection_test2(self, db_name):
        con = sqlite3.connect(db_name)
        cur = con.cursor()

        # cur.execute("CREATE TABLE test3(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        a_file = open("sample_data_tazi.csv")
       
        rows = csv.reader(a_file)
        #rows = next(rows)
        # print("rows test2: ", rows)
        # print("next rows test2: ", next(rows))
        # print("next2 rows test2: ", next(rows))
        next(rows)
        next(rows)
        # print("rows test2: ", rows)
        # cur.executemany("INSERT INTO test3 VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        # cur.execute("SELECT * FROM test3")
        # print("fetch all2 select: ", cur.fetchall())

        query = "DELETE FROM "+db_name
        cur.execute(query)
        print("DELETION check: ", cur.rowcount)
        cur.execute("SELECT * FROM "+db_name)
        print("fetch all2 select after deletion: ", cur.fetchall())
        con.commit()
        con.close()

    
    def database_connection_tazi(self, con, db_name):

        cur = con.cursor()
        a_file = open("sample_data_tazi.csv")
       
        rows = csv.reader(a_file)
        next(rows)
        print("Rows: ", rows)
        print("Rows-next: ", rows)

        #cur.executemany("INSERT INTO "+db_name+" VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        cur.execute("SELECT * FROM "+db_name)
        print("fetch all select: ", cur.fetchall())

        # query = "DELETE FROM "+db_name
        # cur.execute(query)
        # print("DELETION check: ", cur.rowcount)
        # cur.execute("SELECT * FROM "+db_name)
        # print("fetch all2 select after deletion: ", cur.fetchall())

        con.commit()
        con.close()
    
    def database_connection_init(self):
        con = sqlite3.connect(self.db_name+".db")
        return con

    def create_table(self, con, table_name):
        cur = con.cursor()
        cur.execute("CREATE TABLE "+self.db_name+"(id,given_label,model1_A,model1_B,model2_A,model2_B,model3_A,model3_B)")
        con.commit()

    def delete_data(self, cursor):
        query = "DELETE FROM "+self.db_name
        cursor.execute(query)
        print("DELETION check: ", cursor.rowcount)

    def display_table(self):
        cur = con.cursor()
        cur.execute("SELECT * FROM "+self.db_name)
        print("fetch all select: ", cur.fetchall())


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

# d1 = DataSourceManager("sample_data_tazi.csv", "taziSample")
# con = d1.database_connection_init()
# d1.database_connection_tazi("tazi-sample")
#d1.create_table(con, d1.table_name)
#d1.display_table()


