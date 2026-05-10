from airflow.sdk import dag, task

@dag
def versioned_dag(): 
    @task.python
    def first_task():
        print("Hello World")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")

    @task.python
    def version_task():
        print("This is a new task added in the versioned DAG. It will run after the third task.")

    # Set task dependencies
    first_task() >> second_task() >> third_task() >> version_task()

# Instantiate the DAG
versioned_dag()