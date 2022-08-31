from DataSourceManager import DataSourceManager
from ConfusionMatrixManager import ConfusionMatrixManager
from threading import Thread

def populate_data_task(dsm, data_table_name):
    # Part 1: Continuous Data Source
    print("Thread 1 - Populating data task!")
    dsm.db_connection() 
    ## uncomment below to add or delete data
    dsm.pass_from_csv_to_db(data_table_name)
    dsm.display_table(data_table_name)


def sliding_window_data_to_cmm(dsm, conf_mat_table_name, data_table_name):
    # Part 2: Calculating Confusion Matrix
    # this method sends data partitions to confusion matrix manager by sliding window approach
    print("Thread 2 - Sliding Window Task")
    cmm = ConfusionMatrixManager(3, ["A","B"])
    window_range = 10
    db_size_limit = 100
    index = 0
    conf_matrix_array = []
    query = "SELECT COUNT(*) FROM "+data_table_name
    reading_finished = False
    wait_count = 0

    while not reading_finished:
        cur_table_size = dsm.get_table_len(data_table_name)
        print("INFO - Table size:", cur_table_size)
        print("INFO - Thread 2 - Current start_index: ", index)
        print("INFO - Thread 2 - Current end_index: ", index+window_range)

        if cur_table_size == 0:
            print("INFO - Thread 2 - DB is empty!")
        # check if it is end of the db
        elif index+window_range == db_size_limit - 1:
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
            dsm.save_confusion_matrix(conf_mat_table_name, conf_matrix)
            index += 1
        else:
        # if not available data then wait for datasource to populate db
        # 5000 is the waiting constant
            if wait_count < 5000:
                print("\nINFO - ...WAITING...")
                print("INFO - Thread 2 - Current db size is: ", cur_table_size)
                print("INFO - Wait count: ", wait_count)
                wait_count += 1  
            else:
                print("INFO - Thread 2 - Waited too long, so exiting the system.\n")
                reading_finished = True

if __name__ == "__main__":

    filepath = "sample_data_tazi.csv"
    data_table_name = "table13"
    conf_mat_table_name = "conf_matrix13"
    db_name = "Database"

    dsm = DataSourceManager(conf_mat_table_name, db_name) 
    dsm.db_connection()

    ## when the data_table_name and conf_mat_table_name variables are changed, uncomment the lines below. 
    # dsm.create_data_table(data_table_name)
    # dsm.create_conf_matrix_table(conf_mat_table_name)
    
    # clear the tables 
    dsm.delete_data(data_table_name)
    datasource_populating_thread = Thread(target=populate_data_task, args=(dsm, data_table_name))
    confusion_matrix_calculating_thread = Thread(target=sliding_window_data_to_cmm, args=(dsm, conf_mat_table_name, data_table_name))
    datasource_populating_thread.start()
    confusion_matrix_calculating_thread.start()
    datasource_populating_thread.join()
    confusion_matrix_calculating_thread.join()
