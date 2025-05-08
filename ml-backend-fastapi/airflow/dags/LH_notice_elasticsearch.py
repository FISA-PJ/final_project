# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# 모듈 임포트
from plugins.utils.file_helpers import ensure_directory
from plugins.crawlers.lh_crawler import collect_lh_file_urls
from plugins.processors.pdf_processors import PDFProcessor
from plugins.processors.es_uploaders import process_single_pdf
from plugins.processors.es_uploaders import get_elasticsearch_client

# 환경 설정 - 변수 정의
BASE_URL = "https://apply.lh.or.kr"
LIST_URL = f"{BASE_URL}/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch"
DOWNLOAD_URL = f"{BASE_URL}/lhapply/lhFile.do"
DOWNLOAD_DIR = "/opt/airflow/downloads"
HEADERS = {"User-Agent": "Mozilla/5.0"}
ES_INDEX_NAME = Variable.get("ES_INDEX_NAME", "rag-test3")
EMBEDDING_BATCH_SIZE = int(Variable.get("EMBEDDING_BATCH_SIZE", "8"))
MAX_WORKERS = int(Variable.get("MAX_WORKERS", "3"))

# 디렉토리 생성
ensure_directory(DOWNLOAD_DIR)

# 래퍼 함수 - 공고문 URL 수집 및 PDF 다운로드
def collect_urls_wrapper(**kwargs):
    """
    LH 공고문 URL 수집 및 파일 다운로드를 위한 래퍼 함수
    
    이 함수는 Airflow DAG 내에서 직접 호출되어 LH 공고문 크롤링 작업을 수행합니다.
    크롤링 모듈에서 정의된 collect_lh_file_urls 함수를 호출하여 실제 크롤링 작업을 위임합니다.
    수집된 PDF 파일은 DOWNLOAD_DIR에 저장됩니다.
    """
    # Airflow의 execution_date 사용 (실행 스케줄 날짜)
    execution_date = kwargs.get('execution_date', datetime.now()).date()
    print(f"🔄 실행 날짜: {execution_date}")

    return collect_lh_file_urls(BASE_URL, LIST_URL, DOWNLOAD_URL, DOWNLOAD_DIR, HEADERS, execution_date)

# 래퍼 함수 - 저장된 PDF 파일 처리 및 ES 적재
def process_pdfs_wrapper(**kwargs):
    """
    PDF 처리 및 Elasticsearch 업로드를 위한 래퍼 함수
    
    이전 태스크에서 다운로드한 PDF 파일을 처리하여 Elasticsearch에 적재합니다.
    저장된 PDF 파일을 직접 읽어 처리하므로 추가적인 네트워크 요청이 없습니다.
    """
    from concurrent.futures import ThreadPoolExecutor
    from elasticsearch import Elasticsearch
    
    ti = kwargs['ti']
    collected_urls = ti.xcom_pull(task_ids='collect_urls')
    
    if not collected_urls:
        print("⚠️ 수집된 공고가 없습니다. 프로그램을 종료합니다.")
        return
    
    # elasticsearch 클라이언트 생성
    es = get_elasticsearch_client()
    
    # PDF 프로세서 초기화
    processor = PDFProcessor(es, batch_size=EMBEDDING_BATCH_SIZE, index_name=ES_INDEX_NAME)
    
    # 병렬 처리 시작
    print(f"🚀 저장된 PDF 파일 처리 시작 (워커 수: {MAX_WORKERS})")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for _, safe_filename, meta in collected_urls:
            # URL을 사용하지 않고 저장된 파일 경로 사용
            file_path = os.path.join(DOWNLOAD_DIR, safe_filename)
            
            # 파일 존재 확인
            if not os.path.exists(file_path):
                print(f"⚠️ 파일이 존재하지 않습니다: {file_path}")
                continue
                
            # 작업 제출
            future = executor.submit(
                process_single_pdf,
                processor=processor,
                download_dir=DOWNLOAD_DIR,
                safe_filename=safe_filename,
                meta=meta
            )
            futures.append(future)
            if future.result() is True:
                print(f"✅ {safe_filename} 처리 완료")
            else:
                print(f"❌ {safe_filename} 처리 실패")
        # 결과 수집
        success_count = sum(1 for future in futures if future.result() is True)
          
    print(f"✅ PDF 처리 완료: {success_count}/{len(collected_urls)} 성공")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    'lh_notice_elasticsearch_optimized',
    default_args=default_args,
    description='LH 공고문 크롤링 및 Elasticsearch 저장 (최적화 버전)',
    schedule_interval="@daily",
    start_date=datetime(2025, 4, 28),
    catchup=True,
    tags=['lh', 'elasticsearch', 'pdf'],
) as dag:
    # 공고문 URL 수집 및 PDF 다운로드 태스크
    collect_task = PythonOperator(
        task_id='collect_urls',
        python_callable=collect_urls_wrapper,
    )
    
    # 저장된 PDF 처리 및 ES 적재 태스크
    process_task = PythonOperator(
        task_id='process_pdfs',
        python_callable=process_pdfs_wrapper,
    )
    
    # 태스크 의존성 설정
    collect_task >> process_task