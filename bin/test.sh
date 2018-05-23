#!/bin/bash
sbt test
source airflowenv/bin/activate
export PYTHONPATH=pipeline/:$PYTHONPATH
python pipeline/test/weather_uber_rides_int_test_dag.py
deactivate
