from DataSourceManager import DataSourceManager
from Thread_Functions import populate_data_task, sliding_window_data_to_cmm
from threading import Thread

if __name__ == "__main__":

    db_name = "DB1"
    conf_mat_table_name = "CM1"
    data_table_name = "DM1"
    filepath = "sample_data.csv"
    # 100 row of data is in "sample_data.csv"
    db_size_limit = 100

    datasource_populating_thread = Thread(target=populate_data_task, args=(db_name, data_table_name, conf_mat_table_name, filepath, db_size_limit))
    confusion_matrix_calculating_thread = Thread(target=sliding_window_data_to_cmm, args=(db_name, data_table_name, conf_mat_table_name, filepath, db_size_limit))
    datasource_populating_thread.start()
    confusion_matrix_calculating_thread.start()
    datasource_populating_thread.join()
    confusion_matrix_calculating_thread.join()
