from airflow.sdk import dag, task

@dag
def first_dag(): 
    @task.python
    def first_task():
        print("Hello World")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")

    # Set task dependencies
    first_task() >> second_task() >> third_task()

# Instantiate the DAG
first_dag()