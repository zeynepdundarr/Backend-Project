from email import message
import unittest
from DataSourceManager import DataSourceManager
from ConfusionMatrixManager import ConfusionMatrixManager
import pandas as pd
import numpy as np

class Test(unittest.TestCase):

    def setUp(self):
        self.filepath = "test-data.csv"
        self.db_name = "TestDB1"
        self.conf_mat_table_name = "conf_matrix_test"
        self.data_table_name = "TestTable"
        self.window_size = 1
        self.data_table_size = 10
        self.conf_matrix_size = self.data_table_size - self.window_size 
        self.confusion_matrix_str = "(4.0 0.0 6.0 1.0,)"
        self.dsm = DataSourceManager(self.db_name, self.data_table_name, self.conf_mat_table_name, self.filepath)
        self.dsm.db_connection() 
        cur_table_names = self.dsm.display_tables_in_db() 
        self.labels = ['A', 'B']

        if len(cur_table_names) != 0:
            print("TEST - There are existing tables! - ", cur_table_names)
        else:
            self.dsm.create_data_table()
            self.dsm.create_conf_matrix_table()
            cur_table_names = self.dsm.display_tables_in_db(self.data_table_name) 
            print("TEST - Tables are created! - ", cur_table_names)
        
        self.dsm.delete_data(self.data_table_name)
        cur_table_len = self.dsm.get_table_len(self.data_table_name)
        if cur_table_len == 0:
            print("TEST - current table is empty!")
            self.dsm.pass_from_csv_to_db()
        print("TEST - current table is data populated!", self.dsm.get_table_len(self.data_table_name))
        self.dsm.display_table_contents(self.data_table_name)

    def test_pass_from_csv_to_db(self):
        self.dsm.delete_data(self.data_table_name)
        self.dsm.pass_from_csv_to_db()
        query = "SELECT * FROM "+self.data_table_name
        res = self.dsm.get_query_results(query)
        self.assertEqual(self.data_table_size, len(res), message)
    
    def test_insert_data(self):
        self.dsm.delete_data(self.data_table_name)
        with pd.read_csv(self.filepath, chunksize=1, header=None, skiprows = 1) as reader:
            for i, chunk in enumerate(reader):
                chunk = list(chunk.itertuples(index=False, name=None))       
                iterator = map(lambda c: list(c), chunk)
                formatted_chunk= list(iterator)
                self.dsm.insert_data(formatted_chunk)
        self.assertGreater(self.dsm.get_table_len(self.data_table_name), 0, "Insertion to DB is not correct!")

    def test_insert_confusion_matrix(self):
        self.dsm.delete_data(self.conf_mat_table_name)
        self.dsm.insert_confusion_matrix(self.confusion_matrix_str)
        query = "SELECT * FROM "+self.conf_mat_table_name
        res = self.dsm.get_table_len(query)
        self.assertEqual(self.conf_matrix_size, len(res), message)

    def test_create_data_table(self):
        query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        res = self.dsm.get_query_results(query)
        x = [i[0] for i in res]
        if self.data_table_name not in x:
            print("Data table cannot be created!")
        else:
            print("Data table created successfully!")

    def test_create_conf_matrix_table(self):
        query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        res = self.dsm.get_query_results(query)
        x = [i[0] for i in res]
        if self.data_table_name not in x:
            print("Conf matrix table cannot be created!")
        else:
            print("Conf matrix table created successfully!")

    def test_delete_data(self):
        self.dsm.delete_data(self.data_table_name)
        message = "Deletion is incorrect!"
        cur_table_len = self.dsm.get_table_len(self.data_table_name)
        self.assertEqual(cur_table_len, 0, message)
        
    def test_display_table(self):
        self.dsm.delete_data(self.data_table_name)
        self.dsm.pass_from_csv_to_db()
        query = "SELECT * FROM "+self.data_table_name
        res = self.dsm.get_query_results(query)
        message = "Displaying incorrect number of rows!"
        self.assertEqual(self.data_table_size, len(res), message)

    def test_get_query_results(self):
        query = "SELECT * FROM "+self.data_table_name
        res = self.dsm.get_query_results(query)
        message = "get_query_results is incorrect!"
        self.assertEqual(self.data_table_size , len(res), message)
    
    def test_get_table_len(self):
        self.dsm.delete_data(self.data_table_name)
        self.dsm.pass_from_csv_to_db()
        query = "SELECT * FROM "+self.data_table_name
        res = self.dsm.get_query_results(query)
        message = "Getting incorrect table length!"
        self.assertEqual(self.data_table_size , len(res), message)

    def test_save_confusion_matrix(self):
        query = "SELECT * FROM "+self.data_table_name
        self.dsm.delete_data(self.conf_mat_table_name)
        sample_conf_matrix = np.arange(4).reshape((len(self.labels), len(self.labels)))
        self.dsm.save_confusion_matrix(sample_conf_matrix)
        res = self.dsm.get_query_results(query)
        print("Query res: ", res)
        message = "Couldn't save confusion matrix in correct format!"
        self.assertIs(type(res), str, msg=message)

    def test_fetch_confusion_matrix(self):
        self.dsm.delete_data(self.conf_mat_table_name)
        self.dsm.insert_confusion_matrix(self.confusion_matrix_str)
        cur_table_len = self.dsm.get_table_len(self.conf_mat_table_name)
        message = "Cannot fetch confusion matrix!"
        self.assertEqual(cur_table_len, 1, message)

if __name__ == '__main__':
    unittest.main()

