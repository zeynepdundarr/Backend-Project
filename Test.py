from email import message
import unittest
from DataSourceManager import DataSourceManager
from ConfusionMatrixManager import ConfusionMatrixManager

class Test(unittest.TestCase):


    def test_pass_from_csv_to_db(self, dsm, table_name):
        # check if a data is inserted to db
        pass

    def test_db_connection(self):
        #dsm.db_connection()
        pass
    
    def test_insert_data(self, dsm, chunk, table_name):
        dsm.insert_data(chunk, )
        self.assertGreater(dsm.get_table_len(table_name), 0, "Insertion to DB is not correct!")

    def test_insert_confusion_matrix(self, dsm, table_name, confusion_matrix_str):
        pass

    def test_create_data_table(self, dsm, table_name):
        # TEST 100:  [('conf_matrix11',), ('table11',)]
        query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name"
        res = dsm.get_query_results(query)
        x = [i[0] for i in res] 
        self.assertTrue(set(x).issuperset(set(table_name)))
        pass

    def test_create_conf_matrix_table(self, table_name):
       pass

    def test_delete_data(self, dsm, table_name):
        size = dsm.get_table_len(dsm, table_name)
        message = "First and second value is not equal!"
        self.assertEqual(size, 0, message)
        
    def test_display_table(self,table_name):
        pass

    def test_get_query_results(self, query, table_name):
        pass
    
    def test_get_table_len(self, dsm, table_name):
        pass

    def test_save_confusion_matrix(self, table_name, confusion_matrix):
        pass

    def test_fetch_confusion_matrix(self, table_name):
        pass

if __name__ == '__main__':
    filepath = "sample_data_tazi.csv"
    data_table_name = "TestTable"
    conf_mat_table_name = "conf_matrix_test"
    db_name = "Tazi-Test"

    dsm = DataSourceManager(conf_mat_table_name, db_name) 
    dsm.db_connection()
    dsm.create_data_table(data_table_name)
    dsm.create_conf_matrix_table(conf_mat_table_name)

    test = Test()
    test.test_create_data_table(data_table_name)

    #unittest.main()