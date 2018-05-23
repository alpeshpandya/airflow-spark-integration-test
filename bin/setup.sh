#!/bin/bash
pip instal virtualenv
virtualenv airflowenv
source airflowenv/bin/activate
pip install -r pipeline/main/requirements.txt
airflow initdb
deactivate
sbt package
