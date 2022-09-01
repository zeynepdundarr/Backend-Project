from DataSourceManager import DataSourceManager
from ConfusionMatrixManager import ConfusionMatrixManager
from threading import Thread
import numpy as np

def populate_data_task(dsm, data_table_name):
    # Part 1: Continuous Data Source
    print("Thread 1 - Populating data task!")
    dsm.db_connection() 
    query = "SELECT * FROM "+data_table_name
    ## uncomment below to add or delete data
    # dsm.pass_from_csv_to_db()
    dsm.display_table_contents(data_table_name)
    # dsm.get_query_results(query)

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
            print("Conf matrix is: ", conf_matrix)
            conf_matrix_array.append(cmm.calculate_confusion_matrix())
            dsm.save_confusion_matrix(conf_matrix)
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

    # db_name = "DB1"
    # conf_mat_table_name = "CM1"
    # data_table_name = "DM1"
    # filepath = "sample_data.csv"
   
    # dsm = DataSourceManager(db_name, data_table_name, conf_mat_table_name, filepath) 
    # dsm.db_connection()

    # ## when the data_table_name and conf_mat_table_name variables are changed, uncomment the lines below. 
    # #dsm.create_data_table(data_table_name)
    # #dsm.create_conf_matrix_table(conf_mat_table_name)
    
    # dsm.display_tables_in_db()
    # # clear the tables 
    # # dsm.delete_data(data_table_name)
    # datasource_populating_thread = Thread(target=populate_data_task, args=(dsm, data_table_name))
    # confusion_matrix_calculating_thread = Thread(target=sliding_window_data_to_cmm, args=(dsm, conf_mat_table_name, data_table_name))
    
    # datasource_populating_thread.start()
    # confusion_matrix_calculating_thread.start()
    # datasource_populating_thread.join()
    # confusion_matrix_calculating_thread.join()


    # given_label_list 1D array containing A or B
    # 6 prob_values 2D list float values 
    table =  [(1, 'A', 0.6315429094436551, 0.3684570905563449, 0.9881789935400176, 0.0118210064599824, 0.7254980531654877, 0.2745019468345123), 
        (2, 'B', 0.7293816120747935, 0.2706183879252065, 0.0101061883365132, 0.9898938116634868, 0.7539505178154802, 0.2460494821845198), 
        (3, 'A', 0.979591138558099, 0.020408861441901, 0.9205375921041954, 0.0794624078958046, 0.0339680584508004, 0.9660319415491996), 
        (4, 'B', 0.0890861149945974, 0.9109138850054026, 0.7272518420144608, 0.2727481579855392, 0.9409147314255956, 0.0590852685744043), 
        (5, 'B', 0.4501389662697976, 0.5498610337302023, 0.1164488426470428, 0.8835511573529572, 0.2195979863755559, 0.7804020136244441), 
        (6, 'B', 0.8624519236918461, 0.1375480763081539, 0.9840406330873848, 0.0159593669126153, 0.2189237185835933, 0.7810762814164066), 
        (7, 'A', 0.6078003271789233, 0.3921996728210767, 0.8420289090983645, 0.1579710909016355, 0.2798621029211228, 0.7201378970788772), 
        (8, 'A', 0.137643014118616, 0.862356985881384, 0.3866563830533508, 0.6133436169466492, 0.6355385487225098, 0.3644614512774902), 
        (9, 'B', 0.0340706562170941, 0.9659293437829058, 0.1680376539732921, 0.8319623460267078, 0.6356226404777658, 0.3643773595222341), 
        (10, 'B', 0.5094794855363648, 0.4905205144636352, 0.2908688646922155, 0.7091311353077845, 0.0450495751970624, 0.9549504248029376)]

    cmm = ConfusionMatrixManager(3, ["A", "B"])
    given_label_list = np.array(list([row[1] for row in table]))
    prob_values = np.array([np.array(list(map(float,row[2:len(row)])), dtype = np.float32) for row in table])
    labels = ["A", "B"]

    check_given_label_values = False
    cmm.table_divide_and_format(table)
    given_label_set = set(given_label_list)
    print("TEST 0 Label set: ", given_label_set)

    if cmm.given_label_list.ndim == 1:
        print("TEST 1 - CORRECT - given_label_list has dimension 1!")
        for i in labels:
            if i in given_label_set:
                check_given_label_values = True
            else:
                print("Given label values are incorrect!")
                check_given_label_values = False
    else:
        print("TEST 1 - BAD - given_label_list has dimension: ",cmm.given_label_list.ndim)
    
    check_prob_dim = False
    if prob_values.ndim == 2:
        check_prob_dim = True
        print("TEST 2 - CORRECT - prob values has dimension 2!")
        size_prob_row_size = len(prob_values)
        size_prob_col_size = len(prob_values[0])
    else:
        check_prob_dim = False
        print("TEST 2 - BAD - prob values has dimension: ", prob_values.ndim)
    if size_prob_row_size == 10 & size_prob_col_size == 8:
        prob_dim_flag = True
    else:
        prob_dim_flag = False

    message = "Probability values type or given label values are incorrect!"
    self.assertTrue(prob_dim_flag&check_given_label_values, msg=message)
