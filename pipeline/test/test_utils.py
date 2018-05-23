import os
import json
import glob
import pyarrow.parquet as pq
from datetime import date
from main.common_utils import *
from airflow import models
from airflow.settings import Session


def create_tmp_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def check_dir_exists(path):
    return os.path.exists(path)


def get_local_path(path):
    local_path = path
    if local_path.startswith('file://'):
        local_path = local_path[7:]
    return local_path


def test_setup(test_case_class, dag):
    test_case_class.dag = dag
    test_case_class.today = date.today().strftime("%Y%m%d")
    with open('pipeline/resources/config.json', 'r') as f:
        test_case_class.config = json.load(f)

    # Initialize configs
    uber_source_path, uber_ingest_path, uber_transform_datasetId, uber_transform_path = get_pipeline_configs(
        test_case_class.config, test_case_class.today, "uber_data")
    test_case_class.uber_source_path = uber_source_path
    test_case_class.uber_ingest_path = uber_ingest_path
    test_case_class.uber_transform_datasetId = uber_transform_datasetId
    test_case_class.uber_transform_path = uber_transform_path

    weather_source_path, weather_ingest_path, weather_transform_datasetId, weather_transform_path = get_pipeline_configs(
        test_case_class.config, test_case_class.today, "weather_data")
    test_case_class.weather_source_path = weather_source_path
    test_case_class.weather_ingest_path = weather_ingest_path
    test_case_class.weather_transform_datasetId = weather_transform_datasetId
    test_case_class.weather_transform_path = weather_transform_path

    uber_data_path, weather_data_path, app_path, app_dataset = get_app_configs(test_case_class.config,
                                                                               test_case_class.today)
    test_case_class.uber_data_path = uber_data_path
    test_case_class.weather_data_path = weather_data_path
    test_case_class.app_path = app_path
    test_case_class.app_dataset = app_dataset


def verify_schema(path, length, fields):
    files_list = glob.glob(path + '/*.parquet')
    assert len(files_list) > 0
    parquet_table = pq.read_table(files_list[0])
    schema = parquet_table.to_pandas().columns.values.tolist()
    assert len(schema) == length
    for field in fields:
        assert field in schema


def reset(dag_id='humidity_uber_rides'):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()
