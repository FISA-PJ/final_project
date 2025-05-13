# 다중 스테이지 빌드를 사용한 최적화된 Dockerfile

# 기본 이미지
FROM apache/airflow:3.0.0 AS airflow-base

USER root

# 필요한 시스템 패키지 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# 호환성 문제를 해결하기 위해 urllib3 특정 버전 설치
RUN pip install --no-cache-dir urllib3==1.26.18

# 기본 패키지 설치
COPY requirements-base.txt /requirements-base.txt
RUN pip install --no-cache-dir -r /requirements-base.txt

# ---- 경량 서비스용 이미지 (스케줄러, 트리거러, API 서버, 웹서버) ----
FROM airflow-base AS airflow-light
# 이 컨테이너들은 무거운 ML 라이브러리가 필요하지 않음

# ---- 워커 및 DAG 프로세서용 이미지 ----
FROM airflow-base AS airflow-worker-full
# PyTorch 및 AI/ML 관련 패키지 설치
COPY requirements-torch.txt /requirements-torch.txt
# 타임아웃 방지를 위한 옵션 추가 및 여러 단계로 분할하여 설치
RUN pip install --no-cache-dir --timeout=1000 --retries=3 \
    -r /requirements-torch.txt

# pip 패키지 확인
RUN pip freeze | grep urllib3 && \
    pip freeze | grep requests && \
    pip freeze | grep selenium