#!/bin/bash

# 스크립트 실행 시작을 알림
echo "🧹 Airflow 디렉토리 정리를 시작합니다..."

# Airflow 프로젝트 디렉토리 설정
AIRFLOW_PROJECT_DIR="./ml-backend-fastapi/airflow"
LOGS_DIR="${AIRFLOW_PROJECT_DIR}/logs"
DOWNLOADS_DIR="${AIRFLOW_PROJECT_DIR}/downloads"

# logs 디렉토리 정리
echo "📁 logs 디렉토리 정리 중..."
if [ -d "$LOGS_DIR" ]; then
    # 각 서비스 로그 디렉토리 배열
    LOG_SUBDIRS=("apiserver" "scheduler" "worker" "processor" "triggerer")
    
    # 각 서비스 디렉토리의 로그 파일만 삭제
    for subdir in "${LOG_SUBDIRS[@]}"; do
        if [ -d "$LOGS_DIR/$subdir" ]; then
            echo "🗑️ $subdir 로그 파일 정리 중..."
            find "$LOGS_DIR/$subdir" -type f -name "*.log" -delete
            find "$LOGS_DIR/$subdir" -type f -name "*.json" -delete
        fi
    done
    
    # 루트 로그 디렉토리의 로그 파일도 정리
    find "$LOGS_DIR" -maxdepth 1 -type f -name "*.log" -delete
    find "$LOGS_DIR" -maxdepth 1 -type f -name "*.json" -delete
    
    echo "✅ logs 디렉토리가 정리되었습니다."
else
    echo "⚠️ logs 디렉토리를 찾을 수 없습니다."
fi

# downloads 디렉토리 정리
echo "📁 downloads 디렉토리 정리 중..."
if [ -d "$DOWNLOADS_DIR" ]; then
    # 모든 하위 디렉토리와 파일 삭제 (디렉토리 자체는 유지)
    find "$DOWNLOADS_DIR" -mindepth 1 -delete
    echo "✅ downloads 디렉토리가 정리되었습니다."
else
    echo "⚠️ downloads 디렉토리를 찾을 수 없습니다."
fi

# 정리 완료 메시지
echo "🎉 모든 정리가 완료되었습니다!"

mkdir -p ml-backend-fastapi/airflow/logs/{scheduler,worker,apiserver,triggerer,processor}
chmod -R 777 ml-backend-fastapi/airflow/logs