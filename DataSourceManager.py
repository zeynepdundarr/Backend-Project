
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
        self.given_label_list = np.array([row[1] for row in self.table])
        print("TEST 3 - cmm.given: ", cmm.given_label_list)
        self.prob_values = [np.array(list(map(float,row[2:len(row)])), dtype = np.float32) for row in self.table]


    def get_avg_preds(self):
        # TODO: make it scalable for n models
        np_table = np.array(self.prob_values)

        w1, w2, w3 = 0.5, 0.6, 0.7
        weights_label_a = np.array([w1, 0, w2, 0, w3, 0])
        weights_label_b = np.array([0, w1, 0, w2, 0, w3])

        self.avg_probs_a = np.sum(np_table*weights_label_a, axis=1)/3
        self.avg_probs_b = np.sum(np_table*weights_label_b, axis=1)/3
     
    # def confusion_matrix2(actual, pred):

    # labels = ["A", "B"]
    # print("actual: ", actual)
    # print("pred: ", pred)

    # actual = np.array(actual)
    # pred = np.array(pred)

    # labels = np.unique(actual)  
    # matrix = np.zeros((len(labels), len(labels)))

    # for i in range(len(labels)):
    #     for j in range(len(labels)):
    #         matrix[i, j] = np.sum((actual == labels[i]) & (pred == labels[j]))
    #         print("check matrix: ", matrix[i,j])
    # return matrix

    def confusion_matrix(self):

        actual = self.given_label_list
        preds = self.pred_label_list

        labels = np.unique(actual)
        matrix = np.zeros((len(labels), len(labels)))

        for i in range(len(labels)):
            for j in range(len(labels)):
                matrix[i,j] = np.sum( (actual==labels[i]) & (preds==labels[j]))
        return matrix

    def get_pred_list(self):
        res = self.avg_probs_a - self.avg_probs_b
        self.pred_label_list = ["A" if x>=0 else "B" for x in res]

# Run ConfusionMatrixManager
sample_table = [('2', 'B', '0.1', '0.9', '0.2', '0.8', '0.0', '1'),
                ('3', 'B', '0.1', '0.9', '0.2', '0.8', '0.0', '1'),
                ('4', 'B', '0.1', '0.9', '0.2', '0.8', '0.0', '1')]

cmm = ConfusionMatrixManager(sample_table, 3, ["A", "B"])
cmm.table_divide_and_format()
cmm.get_avg_preds()
cmm.get_pred_list()
print("TEST 4 - cmm.given: ", cmm.given_label_list)
print("TEST 0 - Confusion Matrix: ", cmm.confusion_matrix())


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

#cmm = ConfusionMatrixManager()
# res = cmm.get_pred_list(arr1, arr2)
# res = cur.execute("SELECT name FROM sqlite_master")

# arr1 = np.arange(50)
# arr2 = np.arange(50)

# np.random.shuffle(arr1)
# np.random.shuffle(arr2)

# print("arr1-1: ", arr1)


def extract_columns():
    table = [
    ('2', 'B', '0.7293816120747935', '0.27061838792520654', '0.010106188336513222', '0.9898938116634868', '0.7539505178154802', '0.24604948218451983'),
    ('3', 'A', '0.979591138558099', '0.020408861441901016', '0.9205375921041954', '0.07946240789580461', '0.03396805845080042', '0.9660319415491996'),
    ('4', 'B', '0.0890861149945974', '0.9109138850054026', '0.7272518420144608', '0.2727481579855392', '0.9409147314255957', '0.059085268574404326'),
    ('5', 'B', '0.45013896626979766', '0.5498610337302023', '0.1164488426470428', '0.8835511573529572', '0.21959798637555594', '0.7804020136244441'),
    ('6', 'B', '0.8624519236918461', '0.1375480763081539', '0.9840406330873847', '0.015959366912615347', '0.21892371858359339', '0.7810762814164066'),
    ('7', 'A', '0.6078003271789233', '0.3921996728210767', '0.8420289090983645', '0.15797109090163552', '0.2798621029211228', '0.7201378970788772'),
    ('8', 'A', '0.137643014118616', '0.862356985881384', '0.3866563830533508', '0.6133436169466492', '0.6355385487225098', '0.3644614512774902'),
    ('9', 'B', '0.03407065621709415', '0.9659293437829058', '0.16803765397329218', '0.8319623460267078', '0.6356226404777658', '0.36437735952223416'),
    ('10', 'B', '0.5094794855363648', '0.4905205144636352', '0.2908688646922155', '0.7091311353077845', '0.04504957519706243', '0.9549504248029376')]

    col1 = [row[2] for row in table]
    col2 = [row[3] for row in table]
   
    return get_pred_list(col1, col2)


dummy_pred =    ["B", "B", "A", "B", "B", "B", "B", "B", "B", "B"]
dummy_actual =  ["A", "A", "A", "B", "A", "A", "A", "A", "A", "B"]


#print("Confusion Matrix-1: ", confusion_matrix(dummy_actual, dummy_pred))
#print("pred res:", extract_columns())


