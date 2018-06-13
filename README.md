# Sample project demonstrating airflow and spark pipeline

## Pre-requirements
* Python 3
* Scala
* sbt
* Apache Spark

## Functionality
* The pipeline ingests and persists uber rides and weather data sets for period of three months in NYC
* Both data sets are transformed an persisted by transformation stage
* Transformed data is joined, grouped and aggregated to generate data mart

## How-to
* Intall : `./bin/setup.sh`
* Test: `./bin/test.sh`
* Deploy to Airflow
  * `export AIRFLOW_HOME=<path to airflow home>`
  * `./bin/deploy.sh`
* Docker : `./bin/docker_test_run.sh`
