from airflow.sdk import dag, task

@dag
def parallel_dag(): 
    @task.python
    def extract_data(**kwargs):
        print("Extracting data...")
        ti = kwargs['ti']
        extracted_data_dict = {
            "api_data": [1, 2, 3, 4, 5],
            "db_data": [6, 7, 8, 9, 10],
            "s3_data": [11, 12, 13, 14, 15]
        }
        ti.xcom_push(key='return_value', value=extracted_data_dict)
    
    @task.python
    def transform_data_api(**kwargs):
        print("Transforming API data...")
        ti = kwargs['ti']
        api_data = ti.xcom_pull(key='return_value', task_ids='extract_data')['api_data']
        print(f"Transformed API data: {api_data}...")
        transformed_api_data = [x * 10 for x in api_data]
        ti.xcom_push(key='transformed_api_data', value=transformed_api_data)

    @task.python
    def transform_data_db(**kwargs):
        print("Transforming DB data...")
        ti = kwargs['ti']
        db_data = ti.xcom_pull(key='return_value', task_ids='extract_data')['db_data']
        print(f"Transformed DB data: {db_data}...")
        transformed_db_data = [x * 10 for x in db_data]
        ti.xcom_push(key='transformed_db_data', value=transformed_db_data)

    @task.python
    def transform_data_s3(**kwargs):
        print("Transforming S3 data...")
        ti = kwargs['ti']
        s3_data = ti.xcom_pull(key='return_value', task_ids='extract_data')['s3_data']
        print(f"Transformed S3 data: {s3_data}...")
        transformed_s3_data = [x * 10 for x in s3_data]
        ti.xcom_push(key='transformed_s3_data', value=transformed_s3_data)  

    @task.bash
    def load_data(**kwargs):
        print("Loading data into the destination...")
        api_data = kwargs['ti'].xcom_pull(key='transformed_api_data', task_ids='transform_data_api')
        db_data = kwargs['ti'].xcom_pull(key='transformed_db_data', task_ids='transform_data_db')
        s3_data = kwargs['ti'].xcom_pull(key='transformed_s3_data', task_ids='transform_data_s3')

        return f"echo 'Data loaded: API: {api_data}, DB: {db_data}, S3: {s3_data}'"


    # Set task dependencies
    extract_data() >> [transform_data_api(), transform_data_db(), transform_data_s3()] >> load_data()
    

# Instantiate the DAG
parallel_dag()