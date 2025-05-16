# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, helpers
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# process_existing_pdfs 함수 정의
def process_existing_pdfs(**kwargs):
    """
    이미 다운로드된 PDF 파일들을 처리하고 Elasticsearch에 색인하는 함수
    
    다운로드 디렉토리에서 기존에 수집된 PDF 파일들을 모두 읽어 처리합니다.
    """
    from airflow.models import Variable
    import os
    import re
    # PDFProcessor 클래스 임포트
    from plugins.processors.pdf_processors import PDFProcessor
    from plugins.processors.es_uploaders import get_elasticsearch_client
    from plugins.processors.es_uploaders import process_single_pdf
    
    # 환경 변수 및 상수 가져오기
    DOWNLOAD_DIR = "/opt/airflow/downloads"
    EMBEDDING_BATCH_SIZE = int(Variable.get("EMBEDDING_BATCH_SIZE", "8"))
    ES_INDEX_NAME = Variable.get("ES_INDEX_NAME", "rag-test3")
    MAX_WORKERS = int(Variable.get("MAX_WORKERS", "3"))
    
    # 다운로드 디렉토리 확인
    if not os.path.exists(DOWNLOAD_DIR):
        print(f"⚠️ 다운로드 디렉토리가 존재하지 않습니다: {DOWNLOAD_DIR}")
        return
    
    # PDF 파일 목록 가져오기
    pdf_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.lower().endswith('.pdf')]
    if not pdf_files:
        print("⚠️ 처리할 PDF 파일이 없습니다.")
        return
    
    print(f"🔍 총 {len(pdf_files)}개의 PDF 파일을 발견했습니다.")
    
    # Elasticsearch 클라이언트 생성
    try:
        es = get_elasticsearch_client()
        processor = PDFProcessor(es, batch_size=EMBEDDING_BATCH_SIZE, index_name=ES_INDEX_NAME)
    except Exception as e:
        print(f"❌ Elasticsearch 연결 실패: {str(e)}")
        return
    
    # 파일명에서 메타데이터 추출 (패턴: {wrtan_no}_{pub_date}_{filename}.pdf)
    file_data = []
    for file in pdf_files:
        try:
            # 정규식으로 메타데이터 추출
            match = re.match(r'([^_]+)_(\d+)_(.+)\.pdf', file)
            if match:
                wrtan_no, pub_date, short_name = match.groups()
                meta = {
                    'wrtan_no': wrtan_no,
                    'pub_date': pub_date,
                    'filename': f"{short_name}.pdf"
                }
                file_data.append((file, meta))
            else:
                # 패턴이 일치하지 않으면 기본 메타데이터 생성
                meta = {
                    'wrtan_no': 'unknown',
                    'pub_date': '00000000',
                    'filename': file
                }
                file_data.append((file, meta))
        except Exception as e:
            print(f"⚠️ 파일 {file}의 메타데이터 추출 실패: {str(e)}")
    
    # 이미 처리된 파일 확인 (Elasticsearch에 쿼리하여 확인)
    processed_files = set()
    try:
        query = {
            "size": 10000,  # 충분히 큰 값으로 설정
            "query": {"match_all": {}},
            "_source": ["source_pdf"]
        }
        response = es.search(index=ES_INDEX_NAME, body=query)
        for hit in response["hits"]["hits"]:
            processed_files.add(hit["_source"]["source_pdf"])
        
        print(f"📊 이미 처리된 PDF 파일: {len(processed_files)}개")
    except Exception as e:
        print(f"⚠️ 처리된 파일 확인 중 오류: {str(e)}")
    
    # 처리할 파일 필터링 (아직 처리되지 않은 파일만)
    to_process = [(file, meta) for file, meta in file_data if file not in processed_files]
    print(f"🚀 처리할 새 PDF 파일: {len(to_process)}개")
    
    if not to_process:
        print("ℹ️ 모든 PDF 파일이 이미 처리되었습니다.")
        return
    
    # 병렬 처리 시작
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for file, meta in to_process:
            future = executor.submit(
                process_single_pdf,
                processor=processor,
                download_dir=DOWNLOAD_DIR,
                safe_filename=file,
                meta=meta
            )
            futures.append(future)
        
        # 결과 수집
        success_count = sum(1 for future in futures if future.result() is True)
    
    print(f"✅ PDF 처리 완료: {success_count}/{len(to_process)} 성공")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # 더 긴 실행 시간 허용
}

with DAG(
    'Process_Existing_Pdf_Files_To_ElasticSearch',
    default_args=default_args,
    description='기존 저장된 PDF 파일 처리 및 Elasticsearch 적재',
    schedule=None,  # 수동 실행용 (일회성 작업)
    start_date=datetime(2025, 5, 1),
    catchup=False,
    tags=['elasticsearch', 'pdf', 'one-time'],
) as dag:
    # 기존 PDF 처리 태스크
    process_existing_task = PythonOperator(
        task_id='process_existing_pdfs',
        python_callable=process_existing_pdfs,
    )