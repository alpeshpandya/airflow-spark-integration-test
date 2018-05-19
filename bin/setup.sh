sbt package
cp target/scala-2.11/rides_by_humidity_2.11-0.1.0.jar ~/airflow/dags/
cp src/main/airflow/*.py ~/airflow/dags/
cp -R data ~/airflow/dags/
cp src/main/airflow/resources/config.json ~/airflow/dags/
