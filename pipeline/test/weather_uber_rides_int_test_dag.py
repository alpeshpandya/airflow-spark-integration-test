import unittest
import csv
from airflow import models
from datetime import datetime
from main import dag
from test.test_utils import *

DEFAULT_DATE = datetime(2015, 1, 1)


class WeatherUberRidesIntegrationTest(unittest.TestCase):
    def setUp(self):
        test_setup(self, dag)
        # Execute start task as it is dummy task and does not need to be tested
        start_task = self.dag.get_task('start_task')
        task_instance = models.TaskInstance(task=start_task, execution_date=datetime.now())
        start_task.execute(task_instance.get_template_context())

    def test_uber_ingest_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path = get_local_path(self.uber_ingest_path)
        task = self.dag.get_task('cleanup_ingest_uberdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())

        create_tmp_dir(local_path)
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == False

    def test_uber_ingest_spark_task(self):
        "Test to confirm that ingest spark task ingests and persists data"
        local_path = get_local_path(self.uber_ingest_path)
        task = self.dag.get_task('spark_ingest_uberdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == True
        verify_schema(local_path, 6, ['DATE', 'TIME', 'PICK_UP_ADDRESS'])

    def test_uber_transform_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path = get_local_path(self.uber_transform_path)
        task = self.dag.get_task('cleanup_transform_uberdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        create_tmp_dir(local_path)
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == False

    def test_uber_transform_spark_task(self):
        "Test to confirm that spark task transforms and persists data"
        local_path = get_local_path(self.uber_transform_path)
        task = self.dag.get_task('spark_transform_uberdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == True
        verify_schema(local_path, 7, ['dayofweek'])

    def test_weather_ingest_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path = get_local_path(self.weather_ingest_path)
        task = self.dag.get_task('cleanup_ingest_weatherdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        create_tmp_dir(local_path)
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == False

    def test_weather_ingest_spark_task(self):
        "Test to confirm that ingest spark task ingests and persists data"
        local_path = get_local_path(self.weather_ingest_path)
        task = self.dag.get_task('spark_ingest_weatherdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == True
        verify_schema(local_path, 17, ['date', 'hum_avg'])

    def test_weather_transform_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path = get_local_path(self.weather_transform_path)
        task = self.dag.get_task('cleanup_transform_weatherdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        create_tmp_dir(local_path)
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == False

    def test_weather_transform_spark_task(self):
        "Test to confirm that spark task transforms and persists data"
        local_path = get_local_path(self.weather_transform_path)
        task = self.dag.get_task('spark_transform_weatherdata')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == True
        verify_schema(local_path, 19, ['dayofweek', 'humidity_range'])

    def test_join_pipeline_task(self):
        "Dummy test pipeline join"
        task = self.dag.get_task('join_pipeline_task')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        task.execute(task_instance.get_template_context())
        assert 1 == 1

    def test_datamart_cleanup_task(self):
        "Test to confirm that cleanup task removes directory if exists"
        local_path = get_local_path(self.app_path)
        task = self.dag.get_task('cleanup_uber_rides_by_himidity_uberridesbyhumidity')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        create_tmp_dir(local_path)
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == False

    def test_datamart_transform_spark_task(self):
        "Test to confirm that spark task joins and persists data"
        local_path = get_local_path(self.app_path)
        task = self.dag.get_task('spark_uber_rides_by_himidity_uberridesbyhumidity')
        task_instance = models.TaskInstance(task=task, execution_date=datetime.now())
        task.execute(task_instance.get_template_context())
        assert check_dir_exists(local_path) == True
        files_list = glob.glob(local_path + '/*.csv')
        assert len(files_list) == 1
        reader = csv.DictReader(open(files_list[0]))
        schema = reader.fieldnames
        assert 'humidity_range' in schema
        assert 'count' in schema


if __name__ == '__main__':
    reset()
    unittest.main()
