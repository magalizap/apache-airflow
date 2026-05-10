from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
    dag_id="schedule_cron_dag",
    start_date=datetime(year=2026, month=1, day=26, tz="America/Argentina/Buenos_Aires"),
    end_date=datetime(year=2026, month=1, day=31, tz="America/Argentina/Buenos_Aires"),
    schedule=CronTriggerTimetable("0 16 * * MON-FRI", timezone="America/Argentina/Buenos_Aires"),  # Every weekday at 16:00
    is_paused_upon_creation=False,
    catchup=True
)
def schedule_cron_dag(): 
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
schedule_cron_dag()