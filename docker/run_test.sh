#!/bin/bash
rm /airflow-spark-app/pipeline/logs/*.log
sed -i '20s/.*/<value>hdfs:\/\/localhost:8020<\/value>/' /usr/local/hadoop-2.8.2/etc/hadoop/core-site.xml
/usr/local/hadoop-2.8.2/sbin/start-dfs.sh
cd /airflow-spark-app
export PYTHONPATH=pipeline/:$PYTHONPATH
python3 pipeline/test/weather_uber_rides_int_test_dag.py >> /airflow-spark-app/pipeline/logs/unittest.log
