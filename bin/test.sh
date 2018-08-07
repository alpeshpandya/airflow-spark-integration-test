#!/bin/bash
sbt test
sbt package
cp target/scala-2.11/rides_by_humidity_2.11-0.1.0.jar pipeline
cp -R data pipeline
source airflowenv/bin/activate
export PYTHONPATH=pipeline/:$PYTHONPATH
python pipeline/test/weather_uber_rides_int_test_dag.py
deactivate
