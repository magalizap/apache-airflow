from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag
def operator_dag(): 
    @task.python
    def first_task():
        print("Hello World")

    @task.python
    def second_task():
        print("This is the second task")

    @task.bash
    def bash_task_modern() -> str:
        return "echo https://airflow.apache.org/"

    bash_task_oldschool = BashOperator(
        task_id="bash_task_oldschool",
        bash_command="echo https://airflow.apache.org/",
    )

    # Set task dependencies
    first_task() >> second_task() >> bash_task_oldschool >> bash_task_modern()

# Instantiate the DAG
operator_dag()