# dags/lh_crawler_dag.py - DAG 파일 (프로시저 사용)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pytz
import logging

# 크롤러 모듈 임포트
from plugins.crawlers.lh_crawler_for_mysql import collect_lh_notices, save_notices_to_csv

# 로거 설정
logger = logging.getLogger(__name__)

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

# DAG 정의
dag = DAG(
    'LH_Notice_To_Mysql',
    default_args=default_args,
    description='LH 공고문 크롤링 및 저장 DAG (프로시저 사용)',
    schedule= "@daily",
    start_date=datetime(2025, 4, 28),
    catchup=True,
    tags=['crawler', 'LH', 'notices']
)

# 상수 정의
LH_CONFIG = {
    'list_url': 'https://apply.lh.or.kr/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch',
    'headers': {
        'User-Agent': 'Mozilla/5.0'
    }
}

def crawl_lh_notices_task(**context):
    """LH 공고문 크롤링 Task"""
    
    # Airflow의 execution_date 사용 (실행 스케줄 날짜)
    execution_date = context.get('ds')  # ds는 'YYYY-MM-DD' 문자열
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    print(f"🔄 실행 날짜: {execution_date}")

    try:
        # 크롤링 실행 (DB 연결 없이 순수 크롤링만)
        notices_data = collect_lh_notices(
            list_url=LH_CONFIG['list_url'],
            headers=LH_CONFIG['headers'],
            target_date=execution_date
        )
        
        # XCom으로 결과 전달 -> 다음 Task에서 사용
        return notices_data
        
    except Exception as e:
        logger.error(f"❌ 크롤링 중 실패: {str(e)}")
        raise

def process_and_save_notices_task(**context):
    """크롤링 데이터 처리 및 저장 Task (프로시저 사용)"""
    logger.info("크롤링 데이터 처리 및 저장 시작")
    
    # 크롤링 결과 가져오기
    notices_data = context['ti'].xcom_pull(task_ids='crawl_lh_notices')
    
    if not notices_data:
        logger.info("크롤링된 데이터가 없습니다.")
        return {
            'total_crawled': 0,
            'db_saved': 0,
            'csv_saved': 0,
            'errors': 0
        }
    
    # CSV 파일 경로 설정 (주소 없는 공고용)
    # 다운로드 디렉토리 별도 설정
    download_dir = "/opt/airflow/downloads"  # 원하는 경로로 변경
    today_str = context['ds'].replace('-', '')  # YYYYMMDD 형식으로 변환
    csv_file_path = f"{download_dir}/{today_str}.csv"
    
    # 주소 유무에 따라 데이터 분리 및 CSV 저장
    db_notices, csv_notices = save_notices_to_csv(notices_data, csv_file_path)
    
    logger.info(f"데이터 분리 완료 - DB용: {len(db_notices)}개, CSV용: {len(csv_notices)}개")
    
    try:
        # MySQLHook 사용, 연결 ID는 환경변수 AIRFLOW_CONN_NOTICES_DB로 설정됨
        mysql_hook = MySqlHook(mysql_conn_id='notices_db')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # 쿼리 실행
        cursor.execute("SELECT * FROM notices LIMIT 5;")
        rows = cursor.fetchall()

        # 결과 출력
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
    
    db_saved_count = 0
    error_count = 0
    
    # DB에 주소 있는 공고 저장 (제공된 프로시저 사용)
    for notice in db_notices:
        try:
            # 정정공고 여부 판별
            is_correction = notice.get('is_correction', False)
            
            # 정정공고 처리 로직
            if is_correction:
                # 정정공고 프로시저 호출
                mysql_hook.run(
                    sql="CALL ProcessCorrectionNoticeWithHistory(%s, %s, %s, %s, %s, %s)",
                    parameters=(
                        notice['notice_number'],
                        notice['notice_title'],
                        notice['post_date'],
                        notice.get('application_start_date'),
                        notice.get('application_end_date'),
                        notice.get('location')
                    )
                )
                logger.info(f"정정공고 처리 완료: {notice['notice_number']}")
            else:
                # 일반 공고 처리
                mysql_hook.run(
                        sql="CALL InsertNewNotice(%s, %s, %s, %s, %s, %s, %s)",
                        parameters=(
                            notice['notice_number'],
                            notice['notice_title'],
                            notice['post_date'],
                            notice.get('application_start_date'),
                            notice.get('application_end_date'),
                            notice.get('location'),
                            notice.get('is_correction')
                        )
                    )
                logger.info(f"🟢 신규 공고 DB 적재 완료: {notice['notice_number']}")
            
            db_saved_count += 1
            
        except Exception as e:
            logger.error(f"🔴 DB 저장 실패: {notice['notice_number']}, 오류: {e}")
            error_count += 1
    
    # 결과 요약
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
    """크롤링 결과 요약 로그"""
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
    mysql_hook = MySqlHook(mysql_conn_id='notices_db')
    
    # 한국 시간 기준으로 오늘 날짜 계산
    kst = pytz.timezone('Asia/Seoul')
    today = datetime.now(kst).date()
    
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
    task_id='start',
    dag=dag
)

crawl_task = PythonOperator(
    task_id='crawl_lh_notices',
    python_callable=crawl_lh_notices_task,
    dag=dag
)

save_task = PythonOperator(
    task_id='process_and_save_notices',
    python_callable=process_and_save_notices_task,
    dag=dag
)

update_status_task = PythonOperator(
    task_id='update_notice_status',
    python_callable=update_notice_status,
    dag=dag
)

# 요약 로그 Task
summary_task = PythonOperator(
    task_id='log_crawl_summary',
    python_callable=log_crawl_summary,
    trigger_rule='all_done',  # 성공/실패 관계없이 실행
    dag=dag
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag
)

# Task 의존성 설정
start_task >> crawl_task >> save_task >> update_status_task >> summary_task >> end_task

# DAG 문서화
dag.doc_md = """
## LH 공고문 크롤링 DAG (프로시저 사용)

이 DAG는 LH(한국토지주택공사) 공고문을 크롤링하여 처리합니다.

### 주요 기능:
1. **크롤링**: LH 공고문 사이트에서 최신 공고 수집
2. **데이터 분리**: 주소 유무에 따라 처리 방식 결정
   - 주소 있음: MySQL DB에 저장 (프로시저 사용)
   - 주소 없음: CSV 파일로 저장
3. **프로시저 활용**: 
   - ProcessCorrectionNoticeWithHistory: 정정공고 처리
   - InsertNewNotice: 신규공고 삽입
4. **상태 관리**: 접수기간 기반 자동 상태 업데이트

### 실행 스케줄:
- 매일 자정 실행

### 출력:
- **DB**: 주소 정보가 있는 공고 (notices 테이블)
- **CSV**: 주소 정보가 없는 공고 (opt/airflow/downloads/~.csv)

### 정정공고 처리:
- 제목에서 정정 키워드 자동 감지
- 기존 공고가 있으면 ProcessCorrectionNoticeWithHistory 호출
- 없으면 InsertNewNotice로 신규 처리

### 모니터링:
- Airflow UI에서 실행 상태 확인
- crawl_logs 테이블에서 상세 실행 내역 조회
"""