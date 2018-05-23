#!/bin/bash
./bin/test.sh
sbt package
mkdir -p $AIRFLOW_HOME/dags/
cp -R target $AIRFLOW_HOME/dags/
cp pipeline/main/*.py $AIRFLOW_HOME/dags/
cp -R data $AIRFLOW_HOME/dags/
cp -R pipeline/resources $AIRFLOW_HOME/dags/
