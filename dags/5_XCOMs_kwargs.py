from airflow.sdk import dag, task

@dag
def xcoms_dag_kwargs(): 
    @task.python
    def first_task(**kwargs):
        # print kwargs to see the context passed to the task
        print("Task kwargs:", kwargs)
        # Extracting 'ti' (task instance) from kwargs to push XCOM manually
        ti = kwargs['ti']
        print("Extracting data...")
        fetched_data = {"data": [1, 2, 3, 4, 5]}  # Simulated fetched data
        ti.xcom_push(key="extracted_data", value=fetched_data)

    @task.python
    def second_task(**kwargs):
        # Extracting 'ti' (task instance) from kwargs to pull XCOM manually
        ti = kwargs['ti']
        print("Processing extracted data")
        fetched_data = ti.xcom_pull(task_ids="first_task", key="extracted_data")['data']
        processed_data = [x * 2 for x in fetched_data]  # Simulated data processing
        ti.xcom_push(key="processed_data", value={"processed_data": processed_data})

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        print("Loading processed data into destination")
        loaded_data = ti.xcom_pull(task_ids="second_task", key="processed_data")['processed_data']
        print(f"Data to load: {loaded_data}")
        return {"status": "success"}

    # Set task dependencies
    first_task() >> second_task() >> third_task()

# Instantiate the DAG
xcoms_dag_kwargs()