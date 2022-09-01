from DataSourceManager import DataSourceManager
from Thread_Functions import populate_data_task, sliding_window_data_to_cmm
from threading import Thread

if __name__ == "__main__":

    db_name = "DB1"
    conf_mat_table_name = "CM1"
    data_table_name = "DM1"
    filepath = "sample_data.csv"
   
    dsm = DataSourceManager(db_name, data_table_name, conf_mat_table_name, filepath) 
    dsm.db_connection()

    ## when the data_table_name and conf_mat_table_name variables are changed, uncomment the lines below. 
    #dsm.create_data_table(data_table_name)
    #dsm.create_conf_matrix_table(conf_mat_table_name)
    
    dsm.display_tables_in_db()
    # clear the tables 
    # dsm.delete_data(data_table_name)
    datasource_populating_thread = Thread(target=populate_data_task, args=(dsm, data_table_name))
    confusion_matrix_calculating_thread = Thread(target=sliding_window_data_to_cmm, args=(dsm, conf_mat_table_name, data_table_name))
    
    datasource_populating_thread.start()
    confusion_matrix_calculating_thread.start()
    datasource_populating_thread.join()
    confusion_matrix_calculating_thread.join()
