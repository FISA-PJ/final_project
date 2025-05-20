#!/bin/bash

set -euo pipefail

echo "[INFO] Running airflow-init..."

# airflow-init 서비스 실행
docker compose up airflow-init

echo "[INFO] airflow-init completed (first-time run). Starting other services..."

# airflow-init 이후 다른 모든 서비스 실행
docker compose up airflow-* -d

echo "[INFO] All airflow services are now running!"