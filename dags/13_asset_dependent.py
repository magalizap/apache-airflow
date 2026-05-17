from airflow.sdk import dag, task, asset
import os
from asset import fetch_data

@asset(
    schedule=fetch_data,
    # This is optional, but it can be used to specify the output location of the asset
    uri="/opt/airflow/logs/data/data_processed.txt",
    name="process_data"
)
def process_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as f:
        f.write("This is the processed data.\n")
        f.write(f"Data processed succesfully.")

    print(f"Data has been processed and saved to {self.uri}")