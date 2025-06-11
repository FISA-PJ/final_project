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
    # 루트 로그 디렉토리의 로그 파일도 정리
    find "$LOGS_DIR" -mindepth 1 -delete
    echo "✅ logs 디렉토리가 정리되었습니다."
else
    echo "⚠️ logs 디렉토리를 찾을 수 없습니다."
fi

# 정리 완료 메시지
echo "🎉 모든 정리가 완료되었습니다!"