FROM apache/airflow:2.9.2

# 필요한 Python 패키지 설치
USER airflow
RUN pip install --no-cache-dir \
    selenium==4.0.0 \
    requests \
    PyPDF2 \
    scikit-learn \
    elasticsearch \
    torch==2.7.0 \
    transformers \
    langchain \
    openparse