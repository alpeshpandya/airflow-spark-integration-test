from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import date
import json
from factory import pipeline_factory, app_task_factory
import os

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
today = date.today().strftime("%Y%m%d")
dag = DAG('humidity_uber_rides', description='Humidity Range & Uber Rides - Pipeline',
          schedule_interval='0 10 * * *',
          start_date=datetime.now(),
          catchup=False)

with open(DIR_PATH + '/config.json', 'r') as f:
    config = json.load(f)

start_operator = DummyOperator(task_id='start_task', dag=dag)
join_operator = DummyOperator(task_id='join_pipeline_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

upber_data_pipeline = pipeline_factory(dag, config, today, start_operator, 'uber_data')
upber_data_pipeline.set_downstream(join_operator)
weather_data_pipeline = pipeline_factory(dag, config, today, start_operator, 'weather_data')
weather_data_pipeline.set_downstream(join_operator)

app_task = app_task_factory(config, today, dag, join_operator)
app_task.set_downstream(end_task)

start_operator
