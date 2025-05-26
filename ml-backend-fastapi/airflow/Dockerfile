# --------------------- Base 이미지 ---------------------
FROM apache/airflow:3.0.0 AS airflow-base

USER root

# 시스템 패키지 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app \
    && chown -R airflow:root /app

USER airflow

# 기본 requirements 설치
COPY requirements-base.txt /tmp/requirements-base.txt
RUN pip install --no-cache-dir --timeout=1000 --retries=3 \
    -r /tmp/requirements-base.txt

USER root
# pypdf 라이브러리 커스텀 모듈로 덮어쓰기
COPY my_custom_lib/pypdf/_writer.py /tmp/custom_pypdf_writer.py
RUN if [ -f "/tmp/custom_pypdf_writer.py" ]; then \
    PYPDF_PATH=$(python -c "import pypdf; import os; print(os.path.dirname(pypdf.__file__))" 2>/dev/null || echo ""); \
    if [ ! -z "$PYPDF_PATH" ] && [ -d "$PYPDF_PATH" ]; then \
    echo "Found pypdf at: $PYPDF_PATH"; \
    cp /tmp/custom_pypdf_writer.py $PYPDF_PATH/_writer.py; \
    chown airflow:root $PYPDF_PATH/_writer.py; \
    chmod 644 $PYPDF_PATH/_writer.py; \
    echo "pypdf _writer.py custom module applied successfully"; \
    else \
    echo "pypdf not found or path invalid, skipping custom module application"; \
    fi; \
    rm -f /tmp/custom_pypdf_writer.py; \
    else \
    echo "Custom pypdf _writer.py not found, skipping"; \
    fi

# --------------------- 경량 서비스 이미지 ---------------------
FROM airflow-base AS airflow-light


# --------------------- 워커 및 DAG 프로세서 이미지 -------------
FROM airflow-base AS airflow-worker-full

USER airflow
# heavy requirements 설치
COPY requirements-torch.txt /tmp/requirements-torch.txt
RUN pip install --no-cache-dir --timeout=1000 --retries=3 \
    -r /tmp/requirements-torch.txt

USER root
# langchain 라이브러리 커스텀 모듈로 덮어쓰기
COPY my_custom_lib/langchain/retrievers/parent_document_retriever.py /tmp/custom_langchain_retriever.py
RUN if [ -f "/tmp/custom_langchain_retriever.py" ]; then \
    LANGCHAIN_PATH=$(python -c "import langchain; import os; print(os.path.dirname(langchain.__file__))" 2>/dev/null || echo ""); \
    if [ ! -z "$LANGCHAIN_PATH" ] && [ -d "$LANGCHAIN_PATH/retrievers" ]; then \
    echo "Found langchain at: $LANGCHAIN_PATH"; \
    cp /tmp/custom_langchain_retriever.py $LANGCHAIN_PATH/retrievers/parent_document_retriever.py; \
    chown airflow:root $LANGCHAIN_PATH/retrievers/parent_document_retriever.py; \
    chmod 644 $LANGCHAIN_PATH/retrievers/parent_document_retriever.py; \
    echo "langchain parent_document_retriever.py custom module applied successfully"; \
    else \
    echo "langchain not found or retrievers path invalid, skipping custom module application"; \
    fi; \
    rm -f /tmp/custom_langchain_retriever.py; \
    else \
    echo "Custom langchain parent_document_retriever.py not found, skipping"; \
    fi

# 확인용 파이썬 스크립트 복사 및 실행
COPY verify_modules.py /tmp/verify_modules.py
RUN python /tmp/verify_modules.py && rm -f /tmp/verify_modules.py

USER airflow

# 작업 디렉토리 설정
WORKDIR /opt/airflow