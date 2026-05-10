from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
    dag_id="schedule_delta_dag",
    start_date=datetime(year=2026, month=1, day=26, tz="America/Argentina/Buenos_Aires"),
    end_date=datetime(year=2026, month=1, day=31, tz="America/Argentina/Buenos_Aires"),
    schedule=DeltaTriggerTimetable(duration(days=3)),  # Every 3 days
    is_paused_upon_creation=False,
    catchup=True
)
def schedule_delta_dag(): 
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
schedule_delta_dag()