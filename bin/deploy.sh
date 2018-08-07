#!/bin/bash
./bin/test.sh
mkdir -p $AIRFLOW_HOME/dags/
mkdir -p $AIRFLOW_HOME/dags/pipeline
cp -R target/scala-2.11/rides_by_humidity_2.11-0.1.0.jar $AIRFLOW_HOME/dags/pipeline
cp pipeline/main/*.py $AIRFLOW_HOME/dags/
cp -R data $AIRFLOW_HOME/dags/pipeline
cp -R pipeline/resources $AIRFLOW_HOME/dags/
