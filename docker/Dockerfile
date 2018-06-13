FROM singularities/spark
WORKDIR /airflow-spark-app
ADD pipeline /airflow-spark-app/pipeline
ADD bin /airflow-spark-app/pipeline/bin
ADD data /airflow-spark-app/pipeline/data
ADD logs /airflow-spark-app/pipeline/logs
ADD logs /airflow-spark-app/pipeline/logs
ADD target/scala-2.11/rides_by_humidity_2.11-0.1.0.jar /airflow-spark-app/pipeline/rides_by_humidity_2.11-0.1.0.jar
RUN apt-get update
RUN apt-get -y install python3-pip
RUN export AIRFLOW_HOME=~/airflow
RUN pip3 install --no-cache-dir -r /airflow-spark-app/pipeline/main/requirements.txt
RUN airflow initdb
RUN nohup start-spark master &
