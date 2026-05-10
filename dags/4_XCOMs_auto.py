from airflow.sdk import dag, task

@dag
def xcoms_dag_auto(): 
    @task.python
    def first_task():
        print("Extracting data...")
        fetched_data = {"data": [1, 2, 3, 4, 5]}  # Simulated fetched data
        return fetched_data

    @task.python
    def second_task(data: dict):
        print("Processing extracted data")
        proccessed_data = [x * 2 for x in data["data"]]  # Simulated data processing
        return {"processed_data":  proccessed_data}

    @task.python
    def third_task(data: dict):
        print("Loading processed data into destination")
        loaded_data = data["processed_data"]
        print(f"Data to load: {loaded_data}")
        return {"status": "success"}

    # Set task dependencies
    extracted_data = first_task()
    processed_data = second_task(extracted_data)
    third_task(processed_data)

# Instantiate the DAG
xcoms_dag_auto()