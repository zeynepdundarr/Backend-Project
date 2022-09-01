from DataSourceManager import DataSourceManager
from ConfusionMatrixManager import ConfusionMatrixManager
import numpy as np

def populate_data_task(db_name, data_table_name, conf_mat_table_name, filepath, db_size_limit):
    # Part 1: Continuous Data Source
    # This method populate data by 500 chunks
    print("...Thread 1 - Populating data task...")
    dsm = DataSourceManager(db_name, data_table_name, conf_mat_table_name, filepath, db_size_limit) 
    dsm.db_connection()
    ## uncomment below to delete data in the current table
    # dsm.delete_data(data_table_name)
    # DELETE
    dsm.pass_from_csv_to_db()

def sliding_window_data_to_cmm(db_name, data_table_name, conf_mat_table_name, filepath, db_size_limit):
    # Part 2: Calculating Confusion Matrix
    # This method sends data partitions to confusion matrix manager by sliding window approach
    print("...Thread 2 - Sliding Window Task...")
    dsm = DataSourceManager(db_name, data_table_name, conf_mat_table_name, filepath, db_size_limit) 
    cmm = ConfusionMatrixManager(3, ["A","B"])
    window_range = 10
    index = 0
    conf_matrix_array = []
    query = "SELECT COUNT(*) FROM "+data_table_name
    reading_finished = False
    wait_count = 0

    while not reading_finished:
        cur_table_size = dsm.get_table_len(data_table_name)
        if cur_table_size == 0:
            print("INFO - Thread 2 - DB is empty!")
        # check if it is end of the db
        elif index+window_range == dsm.db_size_limit - 1:
            reading_finished = True
            print("INFO - Thread 2 - Reading data is finished!")
        # check if data available for reading in db
        elif index + window_range < cur_table_size:
            print("\nINFO - ...READING...\n")
            wait_count = 0  
            start_index = index
            end_index = index + window_range
            query = "Select * from "+data_table_name+" where cast(id as INT) between "+str(start_index)+" and "+str(end_index)
            window_data = dsm.get_query_results(query)
            cmm.table_divide_and_format(window_data)
            cmm.calculate_avg_preds()
            cmm.calculate_pred_list()
            conf_matrix = cmm.calculate_confusion_matrix()
            conf_matrix_array.append(cmm.calculate_confusion_matrix())
            dsm.save_confusion_matrix(conf_matrix)
            index += 1
        else:
        # if not available data then wait for datasource to populate db
        # 50000 is the waiting constant
            if wait_count < 50000:
                print("\nINFO - ...WAITING...")
                wait_count += 1  
            else:
                print("INFO - Thread 2 - Waited too long, so exiting the system.\n")
                reading_finished = True

