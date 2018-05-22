import unittest
import json
from datetime import date
from airflow.executors import SequentialExecutor, LocalExecutor
from airflow.models import Variable
from airflow import jobs, models, DAG, utils, operators, hooks, macros, settings
from airflow.settings import Session
from datetime import datetime
from weather_uber_rides_dag import dag
from test_utils import *
from common_utils import *


DEFAULT_DATE = datetime(2015, 1, 1)

class WeatherUberRidesIntegrationTest(unittest.TestCase):
    def setUp(self):
        test_setup(self,dag)
        # Execute start task as it is dummy task and does not need to be tested
        self.dag.get_task('start_task').run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.dag.get_task('start_task').run(mark_success=True)

    def test_uber_ingest_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path=get_local_path(self.uber_ingest_path)
        task = self.dag.get_task('cleanup_ingest_uberdata')
        create_tmp_dir(local_path)
        task.test_mode=True
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == False

    def test_uber_ingest_spark_task(self):
        "Test to confirm that ingest spark task ingests and persists data"
        local_path = get_local_path(self.uber_ingest_path)
        task = self.dag.get_task('spark_ingest_uberdata')
        task.test_mode = True
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == True
        verify_schema(local_path,6,['DATE','TIME', 'PICK_UP_ADDRESS'])

    def test_uber_transform_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path=get_local_path(self.uber_transform_path)
        task = self.dag.get_task('cleanup_transform_uberdata')
        task.test_mode = True
        create_tmp_dir(local_path)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == False

    def test_uber_transform_spark_task(self):
        "Test to confirm that spark task transforms and persists data"
        local_path = get_local_path(self.uber_transform_path)
        task = self.dag.get_task('spark_transform_uberdata')
        task.test_mode = True
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == True
        verify_schema(local_path, 7, ['dayofweek'])

    def test_weather_ingest_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path=get_local_path(self.weather_ingest_path)
        task = self.dag.get_task('cleanup_ingest_weatherdata')
        task.test_mode = True
        create_tmp_dir(local_path)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == False

    def test_weather_ingest_spark_task(self):
        "Test to confirm that ingest spark task ingests and persists data"
        local_path = get_local_path(self.weather_ingest_path)
        task = self.dag.get_task('spark_ingest_weatherdata')
        task.test_mode = True
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == True
        verify_schema(local_path,17,['date','hum_avg'])

    def test_weather_transform_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path=get_local_path(self.weather_transform_path)
        task = self.dag.get_task('cleanup_transform_weatherdata')
        task.test_mode = True
        create_tmp_dir(local_path)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == False

    def test_weather_transform_spark_task(self):
        "Test to confirm that spark task transforms and persists data"
        local_path = get_local_path(self.weather_transform_path)
        task = self.dag.get_task('spark_transform_weatherdata')
        task.test_mode = True
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        assert check_dir_exists(local_path) == True
        verify_schema(local_path, 19, ['dayofweek','humidity_range'])

    # def test_join_pipeline_task(self):
    #     "Dummy test to force pipeline join"
    #     # self.dag.get_task('join_pipeline_task').run(start_date=DEFAULT_DATE,
    #     #                                             end_date=DEFAULT_DATE,mark_success=True)
    #     # self.dag.get_task('join_pipeline_task').run(mark_success=True)
    #     join_task = models.TaskInstance(task=self.dag.get_task('join_pipeline_task')
    #                                     , execution_date=datetime.now())
    #     join_task.run()
    #     join_task.set_state(models.State.SUCCESS,Session())
    #     assert 1==1
    #
    # def test_datamart_cleanup_task(self):
    #     "Test to confirm that cleanup task removes directory if exists: cleanup_uber_rides_by_himidity_uberridesbyhumidity"
    #     local_path=get_local_path(self.app_path)
    #     task = self.dag.get_task('cleanup_uber_rides_by_himidity_uberridesbyhumidity')
    #     test_task = models.TaskInstance(task=task
    #                         , execution_date=datetime.now())
    #     create_tmp_dir(local_path)
    #     test_task.run()
    #     assert check_dir_exists(local_path) == False

    # def test_datamart_transform_spark_task(self):
    #     "Test to confirm that spark task joins and persists data"
    #     local_path = get_local_path(self.app_path)
    #     task = self.dag.get_task('spark_uber_rides_by_himidity_uberridesbyhumidity')
    #     task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
    #     assert check_dir_exists(local_path) == True
    #     verify_schema(local_path, 2, ['humidity_range','count'])


def reset(dag_id='humidity_uber_rides'):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()

if __name__ == '__main__':
    reset()
    unittest.main()