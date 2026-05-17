from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    schedule=CronDataIntervalTimetable("@daily",timezone="America/Argentina/Buenos_Aires"),
    start_date=datetime(year=2026, month=1, day=26, tz="America/Argentina/Buenos_Aires"),
    end_date=datetime(year=2026, month=1, day=31, tz="America/Argentina/Buenos_Aires"),
    catchup=True,
)   
def incremental_load():

    @task.python
    def incremental_data_fetch(**kwargs):
        print("Fetching incremental data...")
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")


    @task.bash
    def incremental_data_process():
        return "echo 'Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}'"
    
    incremental_data_fetch() >> incremental_data_process()

# Instantiate the DAG
incremental_load()