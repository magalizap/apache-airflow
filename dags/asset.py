from airflow.sdk import dag, task, asset
import os

@asset(
    schedule="@daily",
    # This is optional, but it can be used to specify the output location of the asset
    uri="/opt/airflow/logs/data/data_extract.txt",
    name="fetch_data"
)
def fetch_data(self):
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, "w") as f:
        f.write("This is the extracted data.\n")
        f.write(f"Data fetched succesfully.")

    print(f"Data has been extracted and saved to {self.uri}")