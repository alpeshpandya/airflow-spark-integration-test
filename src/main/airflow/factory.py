from common_utils import *
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import os

# Constants
# In future use SparkOperator from airflow with new version
SPARK_SUBMIT = 'spark-submit'
MODE = '--deploy-mode client'
INGEST_DRIVER = '--class com.alpesh.integration.batch.ingest.DailyDriver'
TRANSFORM_DRIVER = '--class com.alpesh.integration.batch.transform.DailyDriver'
APP_DRIVER = '--class com.alpesh.integration.batch.app.UberRidesByHumidityRange'
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
APP = DIR_PATH + '/rides_by_humidity_2.11-0.1.0.jar'


def cleanup_task_factory(task_id, path, parent_dag, upstream_task):
    cleanup_path = path
    if cleanup_path.startswith('file://'):
        cleanup_path = cleanup_path[7:]
    print("FACTORY CLEANUP ::::::::::::::::::::::::::: " + cleanup_path+ ' ::: '+task_id)
    cleanup_task = PythonOperator(
        task_id=task_id,
        python_callable=cleanup_dir,
        op_kwargs={'path': cleanup_path},
        dag=parent_dag
    )
    cleanup_task.set_upstream(upstream_task)
    return cleanup_task


def spark_task_factory(task_id, driver, args, parent_dag, upstream_task):
    # Add warehouse directory to arguments
    spark_args = args
    bash_cmd = SPARK_SUBMIT + ' ' + MODE + ' ' + driver + ' ' + APP + ' ' + spark_args
    spark_task = BashOperator(
        task_id=task_id,
        bash_command=bash_cmd,
        retries=3,
        dag=parent_dag
    )
    spark_task.set_upstream(upstream_task)
    return spark_task


def cleanup_spark_component_factory(task_id, clean_path, driver, args, parent_dag, upstream_task):
    cleanup_task = cleanup_task_factory('cleanup_' + task_id
                                        , clean_path
                                        , parent_dag
                                        , upstream_task)

    spark_task = spark_task_factory('spark_' + task_id
                                    , driver
                                    , args
                                    , parent_dag,
                                    cleanup_task)
    return spark_task


def pipeline_factory(parent_dag, config, date_str, upstream_task, data_set_id):
    source_path, ingest_path, transform_datasetId, transform_path = get_pipeline_configs(
        config, date_str, data_set_id)

    ingest_task = cleanup_spark_component_factory(
        'ingest_' + transform_datasetId
        , ingest_path
        , INGEST_DRIVER
        , ' ' + source_path + ' ' + ingest_path
        , parent_dag
        , upstream_task
    )

    transform_task = cleanup_spark_component_factory(
        'transform_' + transform_datasetId
        , transform_path
        , TRANSFORM_DRIVER
        , ' ' + ingest_path + ' ' + transform_path + ' ' + transform_datasetId
        , parent_dag
        , ingest_task
    )
    return transform_task


def app_task_factory(config, date_str, parent_dag, upstream_task):
    uber_data_path, weather_data_path, app_path, app_dataset = get_app_configs(config, date_str)

    app_task = cleanup_spark_component_factory(
        'uber_rides_by_himidity_' + app_dataset
        , app_path
        , APP_DRIVER
        , ' ' + uber_data_path + ' ' + weather_data_path + ' ' + app_path
        , parent_dag
        , upstream_task
    )

    return app_task
