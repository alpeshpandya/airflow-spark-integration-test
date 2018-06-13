#!/bin/bash
docker build -t airflow-spark-app -f docker/Dockerfile  .
cd docker
docker-compose up -d
echo "Test logs reported in logs/unittest.log"
cd ..
