#!/bin/bash
##ACE 태민의 작품

# Airflow 초기화
echo "Running: docker compose up airflow-init"
docker compose up airflow-init

# 모든 airflow 관련 서비스를 백그라운드에서 실행
echo "Running: docker compose up airflow-* -d"
docker compose up airflow-* -d

echo "All commands executed successfully!"
