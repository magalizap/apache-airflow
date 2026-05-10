from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="schedule_preset_dag",
    start_date=datetime(year=2026, month=1, day=1, tz="America/Argentina/Buenos_Aires"),
    schedule="@daily",
    is_paused_upon_creation=False,
    catchup=True
)
def schedule_preset_dag(): 
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
schedule_preset_dag()