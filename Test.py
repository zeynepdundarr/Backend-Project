from email import message
import unittest
from DataSourceManager import DataSourceManager
from ConfusionMatrixManager import ConfusionMatrixManager
import pandas as pd

class Test(unittest.TestCase):
    
    filepath = "test-data.csv"
    data_table_size = 10
    window_size = 1
    conf_matrix_size = data_table_size - window_size 
    test_confusion_matrix = [(1.0, 2.0, 3.0, 4.0)]  
    data_table_name = "TestTable"
    conf_mat_table_name = "conf_matrix_test"
    db_name = "Tazi-Test"

    dsm = DataSourceManager(conf_mat_table_name, db_name) 
    #cmm = ConfusionMatrixManager()

    #dsm.create_data_table(data_table_name)
    #dsm.create_data_table(conf_mat_table_name)  

    def test_pass_from_csv_to_db(self, dsm, table_name):
        dsm.delete_data(table_name)
        dsm.pass_from_csv_to_db(table_name)
        query = "SELECT * FROM "+table_name
        res = dsm.get_query_results(query, table_name)
        self.assertEqual(self.data_table_size, len(res), message)
    
    def test_insert_data(self, dsm, chunk, table_name):
        dsm.delete_data(table_name)
        with pd.read_csv(self.filepath, chunksize=1, header=None, skiprows = 1) as reader:
            for i, chunk in enumerate(reader):
                chunk = list(chunk.itertuples(index=False, name=None))       
                iterator = map(lambda c: list(c), chunk)
                formatted_chunk= list(iterator)
                self.insert_data(formatted_chunk, table_name)
        self.assertGreater(dsm.get_table_len(table_name), 0, "Insertion to DB is not correct!")

    def test_insert_confusion_matrix(self, dsm, table_name, confusion_matrix_str):
        dsm.delete_data(table_name)
        dsm.insert_confusion_matrix()
        query = "SELECT * FROM "+table_name
        res = dsm.get_query_results(self, query, table_name)
        self.assertEqual(self.data_table_size, len(res), message)

    def test_create_data_table(self, dsm, table_name):
        # TEST 100:  [('conf_matrix11',), ('table11',)]
        query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        res = dsm.get_query_results(query, table_name)
        x = [i[0] for i in res] 
        if table_name not in x:
            print("Data table cannot be created!")
        else:
            print("Data table created successfully!")

    def test_create_conf_matrix_table(self, table_name):
        query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        res = dsm.get_query_results(query)
        x = [i[0] for i in res] 
        self.assertTrue(set(x).issuperset(set(table_name)))
        if table_name not in x:
            print("Conf matrix table cannot be created!")
        else:
            print("Conf matrix table created successfully!")

    def test_delete_data(self, dsm, table_name):
        dsm.delete_data(table_name)
        message = "Deletion is incorrect!"
        self.assertEqual(self.data_table_size, 0, message)
        
    def test_display_table(self, dsm, table_name):
        dsm.delete_data(table_name)
        dsm.pass_from_csv_to_db(table_name)
        query = "SELECT * FROM "+table_name
        res = dsm.get_query_results(query, table_name)
        message = "Displaying incorrect number of rows!"
        self.assertEqual(self.data_table_size, len(res), message)

    def test_get_query_results(self, dsm, query, table_name):
        query = "SELECT * FROM "+table_name
        res = dsm.get_query_results(self, query, table_name)
        message = "get_query_results is incorrect!"
        self.assertEqual(self.data_table_size , len(res), message)
    
    def test_get_table_len(self, dsm, table_name):
        dsm.delete_data(table_name)
        dsm.pass_from_csv_to_db(table_name)
        query = "SELECT * FROM "+table_name
        res = dsm.get_query_results(self, query, table_name)
        message = "Getting incorrect table length!"
        self.assertEqual(self.data_table_size , len(res), message)

    def test_save_confusion_matrix(self, dsm, table_name, confusion_matrix):
        dsm.delete_data(table_name)
        dsm.save_confusion_matrix(table_name, confusion_matrix)
        query = "SELECT * FROM "+table_name
        res = dsm.get_query_results(self, query, table_name)
        message = "Couldn't save confusion matrix!"
        self.assertEqual(self.conf_matrix_size, len(res), message)

    def test_fetch_confusion_matrix(self, dsm, table_name):
        query = "SELECT * FROM "+table_name
        dsm.get_query_results(query, table_name)
        res = dsm.get_query_results(query, table_name)
        message = "Cannot fetch confusion matrix!"
        self.assertEqual(self.conf_matrix_size, len(res), message)

if __name__ == '__main__':

    # unittest.main()
    
    t = Test()
    filepath = "test-data.csv"
    data_table_size = 10
    window_size = 1
    conf_matrix_size = data_table_size - window_size 
    test_confusion_matrix = [(1.0, 2.0, 3.0, 4.0)]  
    data_table_name = "TestTable"
    conf_mat_table_name = "conf_matrix_test"
    db_name = "Tazi-Test"

    #dsm.create_data_table(data_table_name)
    #dsm.create_data_table(conf_mat_table_name)  

    dsm = DataSourceManager(conf_mat_table_name, db_name) 
    #t.test_create_data_table(dsm, data_table_name)
    t.test_pass_from_csv_to_db(dsm, data_table_name)
    