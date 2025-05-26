# -*- coding: utf-8 -*-

"""
LH 공고문 크롤링 및 Elasticsearch 저장 DAG

이 DAG는 다음과 같은 주요 기능을 수행합니다:
1. LH 공고 웹사이트에서 새로운 공고문을 크롤링
2. PDF 형식의 공고문 다운로드
3. 다운로드된 PDF 파일의 유효성 검증
4. 유효한 PDF 파일을 Elasticsearch에 저장
5. 처리 결과 모니터링 및 로깅

실행 주기: 매일 1회
"""

from datetime import datetime, timedelta
import subprocess
import os
from elasticsearch import Elasticsearch
import glob
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

# 커스텀 유틸리티 모듈 임포트
from plugins.utils.file_helpers import ensure_directory
from plugins.crawlers.lh_crawler_for_elastic import collect_lh_file_urls_and_pdf
# from plugins.crawlers.lh_crawler_for_elastic import collect_lh_notices_with_address

# 로깅 설정
# 로그 레벨을 INFO로 설정하여 중요한 작업 진행 상황을 추적
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 콘솔 출력을 위한 핸들러 설정
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 크롤링 관련 환경 설정
# BASE_URL: LH 청약센터 기본 URL
# LIST_URL: 공고 목록 페이지 URL
# DOWNLOAD_URL: PDF 다운로드 URL
BASE_URL = "https://apply.lh.or.kr"
LIST_URL = f"{BASE_URL}/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch"
DOWNLOAD_URL = f"{BASE_URL}/lhapply/lhFile.do"

# 파일 시스템 및 처리 관련 설정
DOWNLOAD_DIR = "/opt/airflow/downloads"  # PDF 파일이 저장될 디렉토리
HEADERS = {"User-Agent": "Mozilla/5.0"}  # 크롤링 시 사용할 User-Agent
ES_INDEX_NAME = "rag-test3"  # Elasticsearch 인덱스명
EMBEDDING_BATCH_SIZE = 8  # 임베딩 처리 시 배치 크기
MAX_WORKERS = 3  # Selenium 병렬 처리 워커 수

# 다운로드 디렉토리 생성
ensure_directory(DOWNLOAD_DIR)

def collect_urls_wrapper(**kwargs):
    """
    LH 공고문 URL 수집 및 파일 다운로드를 위한 래퍼 함수
    
    Args:
        **kwargs: Airflow context 변수들을 포함하는 딕셔너리
            - ds: 실행 날짜 (YYYY-MM-DD 형식)
    
    Returns:
        list: 수집된 공고 URL 목록
    
    Note:
        - Airflow의 execution_date를 기준으로 크롤링 수행
        - 수집된 PDF 파일은 DOWNLOAD_DIR에 저장됨
    """
    execution_date = kwargs.get('ds')
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    logger.info(f"🔄 실행 날짜: {execution_date}")

    return collect_lh_file_urls_and_pdf(BASE_URL, LIST_URL, DOWNLOAD_URL, DOWNLOAD_DIR, HEADERS, execution_date)

def get_existing_pdf_files():
    """
    Elasticsearch에서 이미 저장된 PDF 파일들의 목록을 조회
    
    Returns:
        set: 이미 처리된 PDF 파일명들의 집합
    
    Note:
        - Elasticsearch 연결은 Airflow connection을 통해 설정
        - 파일명 중복 처리를 위해 basename만 추출하여 저장
    """
    try:
        # Elasticsearch 연결 설정
        es_conn = BaseHook.get_connection("my_elasticsearch")
        es = Elasticsearch([{'host': es_conn.host, 'port': es_conn.port}])
        
        # ES 연결 상태 확인
        if not es.ping():
            logger.warning("⚠️ Elasticsearch 연결 실패")
            return set()
        
        # source_file 필드만 조회하는 쿼리 실행
        query = {
            "_source": ["source_file"],
            "query": {
                "match_all": {}
            }
        }
        
        response = es.search(index=ES_INDEX_NAME, body=query, size=10000)
        existing_files = set()
        
        # 결과에서 파일명만 추출하여 저장
        for hit in response['hits']['hits']:
            if 'source_file' in hit['_source']:
                existing_files.add(os.path.basename(hit['_source']['source_file']))
        
        return existing_files
    except Exception as e:
        logger.error(f"⚠️ Elasticsearch 조회 중 오류 발생: {str(e)}")
        return set()

def validate_pdf_files(**kwargs):
    """
    다운로드된 PDF 파일들의 유효성을 검사하는 태스크
    
    Args:
        **kwargs: Airflow context 변수들
    
    Returns:
        list: 유효한 PDF 파일명 목록
    
    Note:
        - 이미 처리된 파일은 건너뜀
        - 파일 크기, 형식, 내용 등을 검증
        - 처리 결과는 XCom을 통해 다음 태스크로 전달
    """
    ti = kwargs['ti']
    collected_urls = ti.xcom_pull(task_ids='collect_urls_and_pdfs')
    
    if not collected_urls:
        logger.warning("⚠️ 수집된 공고가 없습니다.")
        return []
    
    # 이미 처리된 파일 목록 조회
    existing_files = get_existing_pdf_files()
    
    # 새로운 PDF 파일 필터링
    all_pdf_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.pdf')]
    new_pdf_files = [f for f in all_pdf_files if f not in existing_files]
    
    if not new_pdf_files:
        logger.info("⚠️ 처리할 새로운 PDF 파일이 없습니다.")
        return []
    
    logger.info(f"🔄 처리할 새로운 PDF 파일 수: {len(new_pdf_files)}")
    
    # 파일 유효성 검사 및 결과 추적
    processed_files = []
    valid_files = []
    
    for file_name in new_pdf_files:
        file_path = os.path.join(DOWNLOAD_DIR, file_name)
        is_valid, error_msg = validate_pdf_file(file_path)
        
        if is_valid:
            valid_files.append(file_name)
            processed_files.append({'name': file_name, 'status': 'pending'})
        else:
            processed_files.append({'name': file_name, 'status': 'failed', 'error': error_msg})
    
    # XCom을 통해 처리 결과 전달
    ti.xcom_push(key='processed_files', value=processed_files)
    ti.xcom_push(key='valid_files', value=valid_files)
    
    return valid_files

def process_valid_files(**kwargs):
    """
    유효성 검사를 통과한 PDF 파일들을 처리하는 태스크
    
    Args:
        **kwargs: Airflow context 변수들
    
    Note:
        - 메모리 사용량 모니터링
        - 외부 프로세서 스크립트 실행
        - 처리 결과 추적 및 로깅
    """
    ti = kwargs['ti']
    valid_files = ti.xcom_pull(task_ids='validate_pdfs', key='valid_files')
    processed_files = ti.xcom_pull(task_ids='validate_pdfs', key='processed_files')
    
    if not valid_files:
        logger.warning("⚠️ 유효한 PDF 파일이 없습니다.")
        return
    
    # 메모리 사용량 모니터링
    monitor_memory_usage()
    
    # 환경변수로 처리할 파일 목록 전달
    os.environ['PROCESS_PDF_FILES'] = ','.join(valid_files)
    
    try:
        # 외부 프로세서 스크립트 실행
        result = subprocess.run(
            ['python', '/opt/airflow/plugins/processors/main.py'],
            capture_output=True,
            text=True
        )

        # 실행 결과 로깅
        logger.info("📄 [STDOUT]:\n" + result.stdout)
        if result.stderr:
            logger.error("⚠️ [STDERR]:\n" + result.stderr)

        if result.returncode != 0:
            raise Exception(f"Script failed with return code {result.returncode}")
        
        # 성공 처리
        for file in processed_files:
            if file['name'] in valid_files:
                file['status'] = 'success'
        
    except Exception as e:
        # 실패 처리
        for file in processed_files:
            if file['name'] in valid_files and file['status'] == 'pending':
                file['status'] = 'failed'
                file['error'] = str(e)
        raise
    
    finally:
        # 최종 처리 결과 전달
        ti.xcom_push(key='final_processed_files', value=processed_files)

def cleanup_and_report(**kwargs):
    """
    처리 결과 로깅 및 정리 작업을 수행하는 태스크
    
    Args:
        **kwargs: Airflow context 변수들
    
    Note:
        - 처리 결과 상세 로깅
        - 메모리 사용량 최종 확인
        - 임시 파일 정리
    """
    ti = kwargs['ti']
    processed_files = ti.xcom_pull(task_ids='process_files', key='final_processed_files')
    
    # 처리 결과 로깅
    log_processing_results(processed_files)
    
    # 메모리 사용량 최종 확인
    monitor_memory_usage()
    
    # 임시 파일 정리
    cleanup_temp_files()

def monitor_memory_usage():
    """
    현재 프로세스의 메모리 사용량을 모니터링하고 로깅
    
    Note:
        - psutil을 사용하여 현재 프로세스의 메모리 사용량 측정
        - MB 단위로 변환하여 로깅
    """
    import psutil
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / 1024 / 1024  # MB 단위로 변환
    logger.info(f"📊 현재 메모리 사용량: {memory_usage:.2f} MB")

def log_processing_results(processed_files):
    """
    파일 처리 결과를 상세하게 로깅
    
    Args:
        processed_files (list): 처리된 파일들의 상태 정보
    
    Note:
        - 전체 처리 결과 요약
        - 실패한 파일들에 대한 상세 정보 로깅
    """
    if not processed_files:
        logger.warning("처리된 파일이 없습니다.")
        return
        
    success_count = len([f for f in processed_files if f['status'] == 'success'])
    failed_count = len([f for f in processed_files if f['status'] == 'failed'])
    
    logger.info(f"""
        📋 처리 결과 요약:
        - 전체 파일 수: {len(processed_files)}
        - 성공: {success_count}
        - 실패: {failed_count}
        """)
    
    if failed_count > 0:
        logger.error("❌ 실패한 파일들:")
        for file in processed_files:
            if file['status'] == 'failed':
                logger.error(f"- {file['name']}: {file['error']}")

def validate_pdf_file(file_path):
    """
    개별 PDF 파일의 유효성을 검사
    
    Args:
        file_path (str): 검사할 PDF 파일의 경로
    
    Returns:
        tuple: (bool, str) - (유효성 여부, 오류 메시지)
    
    Note:
        - 파일 존재 여부 확인
        - 파일 확장자 검증
        - 파일 크기 확인
        - PDF 형식 검증
    """
    try:
        if not os.path.exists(file_path):
            return False, "파일이 존재하지 않습니다"
        
        if not file_path.lower().endswith('.pdf'):
            return False, "PDF 파일이 아닙니다"
        
        if os.path.getsize(file_path) == 0:
            return False, "빈 파일입니다"
        
        # PDF 파일 형식 검사
        import PyPDF2
        with open(file_path, 'rb') as f:
            try:
                PyPDF2.PdfReader(f)
                return True, None
            except:
                return False, "유효하지 않은 PDF 파일입니다"
                
    except Exception as e:
        return False, f"파일 검사 중 오류 발생: {str(e)}"

def cleanup_temp_files():
    """
    처리 완료 후 임시 파일들을 정리
    
    Note:
        - 임시 파일 패턴: *.tmp, *.temp, *.log
        - 삭제 실패 시 경고 로그 기록
    """
    temp_patterns = ['*.tmp', '*.temp', '*.log']
    for pattern in temp_patterns:
        for file_path in glob.glob(os.path.join(DOWNLOAD_DIR, pattern)):
            try:
                os.remove(file_path)
                logger.info(f"🗑️ 임시 파일 삭제: {file_path}")
            except Exception as e:
                logger.warning(f"⚠️ 임시 파일 삭제 실패: {file_path} - {str(e)}")

def run_main_script(**kwargs):
    """
    메인 처리 스크립트를 실행하는 함수
    
    Args:
        **kwargs: Airflow context 변수들
    
    Note:
        - 새로운 PDF 파일 필터링
        - 외부 프로세서 스크립트 실행
        - 오류 발생 시 예외 처리 및 로깅
    """
    ti = kwargs['ti']
    collected_urls = ti.xcom_pull(task_ids='collect_urls_and_pdfs')
    
    if not collected_urls:
        logger.info("⚠️ 수집된 공고가 없습니다. 프로그램을 종료합니다.")
        return
    
    # 이미 처리된 파일 필터링
    existing_files = get_existing_pdf_files()
    all_pdf_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.pdf')]
    new_pdf_files = [f for f in all_pdf_files if f not in existing_files]
    
    if not new_pdf_files:
        logger.info("⚠️ 처리할 새로운 PDF 파일이 없습니다.")
        return
    
    logger.info(f"🔄 처리할 새로운 PDF 파일 수: {len(new_pdf_files)}")
    
    # 환경변수로 처리할 파일 목록 전달
    os.environ['PROCESS_PDF_FILES'] = ','.join(new_pdf_files)
    
    try:
        # 메인 스크립트 실행
        result = subprocess.run(
            ['python', '/opt/airflow/plugins/processors/main.py'],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(result.stdout)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Script failed with error: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

# DAG 기본 설정
default_args = {
    'owner': 'airflow',  # DAG 소유자
    'depends_on_past': False,  # 이전 실행 결과에 의존하지 않음
    'email_on_failure': False,  # 실패 시 이메일 알림 비활성화
    'email_on_retry': False,  # 재시도 시 이메일 알림 비활성화
    'retries': 3,  # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 간격
    'execution_timeout': timedelta(hours=5),  # 최대 실행 시간
}

# DAG 정의
with DAG(
    'LH_Notice_To_ElasticSearch',  # DAG ID
    default_args=default_args,
    description='LH 공고문 크롤링 및 Elasticsearch 저장',
    schedule="@daily",  # 매일 실행
    start_date=datetime(2025, 1, 1),  # 시작 날짜
    catchup=True,  # 과거 날짜에 대한 백필 활성화
    tags=['lh', 'elasticsearch', 'pdf'],  # DAG 태그
) as dag:
    # Task 1: 공고문 URL 수집 및 PDF 다운로드
    collect_task = PythonOperator(
        task_id='collect_urls_and_pdfs',
        python_callable=collect_urls_wrapper,
    )

    # Task 2: PDF 파일 유효성 검사
    validate_task = PythonOperator(
        task_id='validate_pdfs',
        python_callable=validate_pdf_files,
    )

    # Task 3: PDF 처리 및 ES 적재
    process_task = PythonOperator(
        task_id='process_files',
        python_callable=process_valid_files,
    )

    # Task 4: 정리 및 결과 보고
    cleanup_task = PythonOperator(
        task_id='cleanup_and_report',
        python_callable=cleanup_and_report,
    )
    
    # 태스크 의존성 설정 (실행 순서 정의)
    collect_task >> validate_task >> process_task >> cleanup_task