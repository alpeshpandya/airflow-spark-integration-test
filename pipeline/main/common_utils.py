import os
import shutil

DIR_PATH = os.getcwd()+'/'


def cleanup_dir(path):
    if os.path.exists(path) and os.path.isdir(path):
        shutil.rmtree(path)
    return path


def get_pipeline_configs(config, date_str, data_set_id):
    targetMap = config['ingest']['output']['hdfs']['dataSets']
    source_path = DIR_PATH + config['ingest']['sources'][data_set_id]
    ingest_path = DIR_PATH + config['ingest']['output']['hdfs']['uri'] % (config['common']['hdfs']['lake1Path']
                                                                          , targetMap[data_set_id]
                                                                          , date_str)
    transform_datasetId = config['transform']['hdfs']['dataSets'][data_set_id]
    transform_path = DIR_PATH + config['transform']['hdfs']['uri'] % (config['common']['hdfs']['lake2Path']
                                                                      , transform_datasetId
                                                                      , date_str)
    return source_path, ingest_path, transform_datasetId, transform_path


def get_app_configs(config, date_str):
    uber_transform_datasetId = config['transform']['hdfs']['dataSets']['uber_data']
    uber_data_path = DIR_PATH + config['transform']['hdfs']['uri'] % (config['common']['hdfs']['lake2Path']
                                                                      , uber_transform_datasetId
                                                                      , date_str)
    weather_transform_datasetId = config['transform']['hdfs']['dataSets']['weather_data']
    weather_data_path = DIR_PATH + config['transform']['hdfs']['uri'] % (config['common']['hdfs']['lake2Path']
                                                                         , weather_transform_datasetId
                                                                         , date_str)
    app_dataset = config['app']['dataset']
    app_path = DIR_PATH + config['app']['uri'] % (config['common']['hdfs']['lake3Path']
                                                  , app_dataset, date_str)
    return uber_data_path, weather_data_path, app_path, app_dataset
