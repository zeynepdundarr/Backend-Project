import pandas as pd
from time import sleep, perf_counter
from threading import Thread

class DataSourceManager:

    def __init__(self, filepath):
        self.filepath = filepath

    def read_from_csv(self):

        chunksize = 50000
        with pd.read_csv(self.filepath, chunksize=chunksize) as reader:
            for i, chunk in enumerate(reader):
                print("%d th chunk" %i)
                # TODO: send it to database
                # end 
                sleep(1)
        
    
    def populate_data_task(self):
        print("Start populating data...")
        self.read_from_csv()

    def calculation_data_task(self):
        print("Start calculating data...")
        sleep(5)


d1 = DataSourceManager("tazi-se-interview-project-data.csv")
start_time = perf_counter()
t1 = Thread(target=d1.populate_data_task)
t2 = Thread(target=d1.calculation_data_task)

t1.start()
t2.start()

t1.join()
t2.join()

end_time = perf_counter()

print(f"It took {end_time-start_time: 0.2f} secs to complete.")

