# -*- coding: utf-8 -*-

"""
LH 공고문 크롤링 및 MySQL 저장 DAG

이 DAG는 다음과 같은 주요 기능을 수행합니다:
1. LH 공고 웹사이트에서 새로운 공고문을 크롤링
2. 주소 정보 유무에 따라 데이터 분리
3. 주소가 있는 공고는 MySQL DB에 저장 (프로시저 사용)
4. 주소가 없는 공고는 CSV 파일로 저장
5. 공고 상태 자동 업데이트 (접수중/접수마감)
6. 처리 결과 모니터링 및 로깅

실행 주기: 매일 1회
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pytz
import logging

# 크롤러 모듈 임포트
from plugins.crawlers.lh_crawler_for_mysql import (
    collect_lh_notices, 
    classify_notices_by_completeness
)

# 로깅 설정
# 로그 레벨을 INFO로 설정하여 중요한 작업 진행 상황을 추적
logger = logging.getLogger(__name__)

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
dag = DAG(
    'LH_Notice_To_Mysql',  # DAG ID
    default_args=default_args,
    description='LH 공고문 크롤링 및 저장 DAG (프로시저 사용)',
    schedule= "@daily",  # 매일 실행
    start_date=datetime(2025, 5, 19),  # 시작 날짜
    catchup=True,  # 과거 날짜에 대한 백필 활성화
    tags=['crawler', 'LH', 'notices']  # DAG 태그
)

# 크롤링 설정
LH_CONFIG = {
    'list_url': 'https://apply.lh.or.kr/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch',  # 공고 목록 페이지
    'headers': {
        'User-Agent': 'Mozilla/5.0'  # 크롤링 시 사용할 User-Agent
    }
}

def crawl_lh_notices_task(**context):
    """
    LH 공고문 크롤링 Task
    
    Args:
        **context: Airflow context 변수들을 포함하는 딕셔너리
            - ds: 실행 날짜 (YYYY-MM-DD 형식)
    
    Returns:
        list: 수집된 공고 데이터 목록
    
    Note:
        - 실행 날짜를 기준으로 해당 일자의 공고만 수집
        - DB 연결 없이 순수 크롤링 작업만 수행
        - 결과는 XCom을 통해 다음 Task로 전달
    """
    execution_date = context.get('ds')
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    print(f"🔄 실행 날짜: {execution_date}")

    try:
        notices_data = collect_lh_notices(
            list_url=LH_CONFIG['list_url'],
            headers=LH_CONFIG['headers'],
            target_date=execution_date
        )
        return notices_data
        
    except Exception as e:
        logger.error(f"❌ 크롤링 Task 중 실패: {str(e)}")
        raise

def process_and_save_notices_task(**context):
    """
    크롤링 데이터 처리 및 저장 Task
    
    Args:
        **context: Airflow context 변수들
    
    Returns:
        dict: 처리 결과 통계
            - total_crawled: 전체 크롤링된 공고 수
            - db_saved: DB에 저장된 공고 수
            - csv_saved: CSV에 저장된 공고 수
            - errors: 오류 발생 건수
            - csv_file: CSV 파일 경로
    
    Note:
        - 주소 정보 유무에 따라 데이터 분리
        - 주소 있는 공고는 DB에 저장 (프로시저 사용)
        - 주소 없는 공고는 CSV 파일로 저장
        - 정정공고 여부를 확인하여 적절한 프로시저 호출
    """
    logger.info("크롤링 데이터 처리 및 저장 시작")
    
    # 이전 Task에서 크롤링한 데이터 가져오기
    notices_data = context['ti'].xcom_pull(task_ids='crawl_lh_notices')
    
    if not notices_data:
        logger.info("크롤링된 데이터가 없습니다.")
        return {
            'total_crawled': 0,
            'db_saved': 0,
            'csv_saved': 0,
            'errors': 0
        }
    
    # CSV 파일 경로 설정
    download_dir = "/opt/airflow/downloads/no_location_notice"
    today_str = context['ds'].replace('-', '')
    csv_file_path = f"{download_dir}/{today_str}.csv"
    
    # 주소 정보 유무에 따라 데이터 분리
    db_notices, csv_notices = classify_notices_by_location(notices_data, csv_file_path)
    logger.info(f"데이터 분리 완료 - DB용: {len(db_notices)}개, CSV용: {len(csv_notices)}개")
    
    try:
        # MySQL 연결 설정
        mysql_hook = MySqlHook(mysql_conn_id='notices_db')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # 연결 확인
        logger.info("MySQL 연결 확인 쿼리 실행")
        cursor.execute("SELECT * FROM notices LIMIT 5;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)

        # 연결 정보 로깅
        conn_info = mysql_hook.get_connection('notices_db')
        logger.info("✅ MySQL 연결 성공!")
        logger.info(f"🏠 Host: {conn_info.host}")
        logger.info(f"🔌 Port: {conn_info.port}")
        logger.info(f"💾 Database: {conn_info.schema}")
        
    except Exception as e:
        logger.error(f"❌ MySQL 연결 실패: {str(e)}")
        raise   
    
    # 처리 결과 카운터 초기화
    db_saved_count = 0
    error_count = 0

    # 작업 실행 시간 설정 (한국 시간)
    execution_date = context.get('logical_date') or context.get('execution_date')
    job_execution_time = execution_date.strftime('%Y-%m-%d %H:%M:%S')

    # DB에 공고 저장
    for notice in db_notices:
        try:
            # 정정공고 여부 확인
            is_correction = notice.get('is_correction')
            
            if is_correction:
                # 정정공고 처리
                mysql_hook.run(
                    sql="CALL ProcessCorrectionNotice(%s, %s, %s, %s, %s, %s, %s)",
                    parameters=(
                        notice['notice_number'],
                        notice['notice_title'],
                        notice['post_date'],
                        notice.get('application_start_date'),
                        notice.get('application_end_date'),
                        notice.get('location'),
                        job_execution_time
                    )
                )
                logger.info(f"정정공고 처리 완료: {notice['notice_number']}")
            else:
                # 일반 공고 처리
                mysql_hook.run(
                    sql="CALL InsertNewNotice(%s, %s, %s, %s, %s, %s, %s, %s)",
                    parameters=(
                        notice['notice_number'],
                        notice['notice_title'],
                        notice['post_date'],
                        notice.get('application_start_date'),
                        notice.get('application_end_date'),
                        notice.get('location'),
                        notice.get('is_correction'),
                        job_execution_time
                    )
                )
                logger.info(f"🟢 신규 공고 DB 적재 완료: {notice['notice_number']}")
            
            db_saved_count += 1
            
        except Exception as e:
            logger.error(f"🔴 DB 저장 실패: {notice['notice_number']}, 오류: {e}")
            error_count += 1
    
    # 처리 결과 반환
    result = {
        'total_crawled': len(notices_data),
        'db_saved': db_saved_count,
        'csv_saved': len(csv_notices),
        'errors': error_count,
        'csv_file': csv_file_path
    }
    
    logger.info(f"저장 완료 - DB: {db_saved_count}건, CSV: {len(csv_notices)}건, 오류: {error_count}건")
    return result

def log_crawl_summary(**context):
    """
    크롤링 결과 요약 로깅
    
    Args:
        **context: Airflow context 변수들
    
    Note:
        - 전체 처리 결과 통계 출력
        - 성공률 계산 및 표시
        - CSV 파일 위치 정보 제공
    """
    result = context['ti'].xcom_pull(task_ids='process_and_save_notices')
    
    if result:
        success_rate = ((result['db_saved'] + result['csv_saved']) / result['total_crawled'] * 100) if result['total_crawled'] > 0 else 0
        
        summary = f"""
        ========= LH 크롤링 완료 요약 =========
        실행일시: {context['ds']}
        DAG Run ID: {context['dag_run'].run_id}
        
        📊 처리 결과:
        - 총 크롤링: {result['total_crawled']}건
        - DB 저장 (주소 있음): {result['db_saved']}건
        - CSV 저장 (주소 없음): {result['csv_saved']}건
        - 오류 발생: {result['errors']}건
        
        📁 파일 위치:
        - CSV 파일: {result.get('csv_file', 'N/A')}
        
        ✅ 성공률: {success_rate:.1f}%
        =====================================
        """
        
        logger.info(summary)
        print(summary)

def update_notice_status(**context):
    """
    공고 상태 자동 업데이트
    
    Args:
        **context: Airflow context 변수들
    
    Note:
        - 한국 시간 기준으로 현재 날짜 확인
        - 접수 기간에 따라 상태 자동 업데이트
            - 접수기간 중: '접수중'
            - 접수기간 종료: '접수마감'
        - 접수 시작일/종료일이 없는 공고는 제외
    """
    mysql_hook = MySqlHook(mysql_conn_id='notices_db')
    
    # 한국 시간 기준으로 오늘 날짜 계산
    kst = pytz.timezone('Asia/Seoul')
    today = datetime.now(kst).date()
    
    # 상태 업데이트 쿼리 실행
    sql = """
        UPDATE notices 
        SET notice_status = CASE 
                WHEN %s BETWEEN application_start_date AND application_end_date THEN '접수중'
                WHEN %s > application_end_date THEN '접수마감'
                ELSE '접수중'
            END,
            updated_at = CURRENT_TIMESTAMP
        WHERE application_start_date IS NOT NULL
        AND application_end_date IS NOT NULL
    """
    
    mysql_hook.run(sql, parameters=(today, today))

# Task 정의
start_task = EmptyOperator(
    task_id='start',  # DAG 시작 지점
    dag=dag
)

crawl_task = PythonOperator(
    task_id='crawl_lh_notices',  # 크롤링 작업
    python_callable=crawl_lh_notices_task,
    dag=dag
)

# save_task = PythonOperator(
#     task_id='process_and_save_notices',  # 데이터 처리 및 저장
#     python_callable=process_and_save_notices_task,
#     dag=dag
# )

# update_status_task = PythonOperator(
#     task_id='update_notice_status',  # 공고 상태 업데이트
#     python_callable=update_notice_status,
#     dag=dag
# )

# summary_task = PythonOperator(
#     task_id='log_crawl_summary',  # 처리 결과 요약
#     python_callable=log_crawl_summary,
#     trigger_rule='all_done',  # 이전 태스크 성공/실패 관계없이 실행
#     dag=dag
# )

end_task = EmptyOperator(
    task_id='end',  # DAG 종료 지점
    dag=dag
)

# Task 의존성 설정 (실행 순서 정의)
# start_task >> crawl_task >> save_task >> update_status_task >> summary_task >> end_task
start_task >> crawl_task >> end_task

# DAG 문서화
dag.doc_md = """
## LH 공고문 크롤링 DAG

이 DAG는 LH(한국토지주택공사) 공고문을 크롤링하여 처리합니다.

### 주요 기능:
1. **크롤링**: LH 공고문 사이트에서 최신 공고 수집
2. **데이터 분리**: 주소 유무에 따라 처리 방식 결정
   - 주소 있음: MySQL DB에 저장 (프로시저 사용)
   - 주소 없음: CSV 파일로 저장
3. **프로시저 활용**: 
   - ProcessCorrectionNotice: 정정공고 처리
   - InsertNewNotice: 신규공고 삽입
4. **상태 관리**: 접수기간 기반 자동 상태 업데이트

### 실행 스케줄:
- 매일 자정 실행

### 출력:
- **DB**: 주소 정보가 있는 공고 (notices 테이블)
- **CSV**: 주소 정보가 없는 공고 (opt/airflow/downloads/~.csv)

### 정정공고 처리:
- 제목에서 정정 키워드 자동 감지
- 기존 공고가 있으면 ProcessCorrectionNotice 호출
- 없으면 InsertNewNotice로 신규 처리

### 시간 정보:
- 각 공고의 created_at 필드는 DAG 작업 실행 시간으로 설정됨
- 이를 통해 어떤 배치 작업에서 해당 공고가 처리되었는지 추적 가능

### 모니터링:
- Airflow UI에서 실행 상태 확인
- crawl_logs 테이블에서 상세 실행 내역 조회
"""