#!/bin/bash

set -euo pipefail

echo "[INFO] Running airflow-init..."

# airflow-init 서비스 실행
docker compose up airflow-init

echo "[INFO] airflow-init completed (first-time run). Starting other services..."

# 다른 airflow-* 서비스 실행
docker compose up $(docker compose config --services | grep '^airflow-') -d

echo "[INFO] All airflow services are now running!"