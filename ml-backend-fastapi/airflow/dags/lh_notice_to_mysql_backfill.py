# -*- coding: utf-8 -*-

"""
LH 공고문 백필 DAG - 간단 버전

기존 크롤링 함수의 target_date만 바꿔가며 과거 데이터 수집
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
import time
import json
from typing import List, Dict

# 실제 크롤링 함수 임포트
from plugins.crawlers.lh_crawler_for_mysql import (
    collect_lh_notices, 
    classify_notices_by_completeness
)

logger = logging.getLogger(__name__)

# 백필 설정
BACKFILL_CONFIG = {
    'start_date': datetime(2025, 1, 1).date(),      # 시작일
    'end_date': datetime(2025, 5, 26).date(),       # 종료일 (어제까지)
    'delay_between_dates': 3,                       # 날짜 간 3초 대기
    'skip_weekends': True,                          # 주말 제외 여부
    'batch_size': 30,                               # 30일마다 중간 보고
}

# LH 크롤링 설정  
LH_CONFIG = {
    'list_url': 'https://apply.lh.or.kr/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch',
    'headers': {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
}

# DAG 정의
backfill_dag = DAG(
    'LH_Notice_To_Mysql_Simple_Backfill',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=12),  # 최대 12시간
    },
    description='LH 공고문 간단 백필 DAG',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['crawler', 'LH', 'backfill', 'simple']
)

def run_backfill_crawling(**context):
    """
    날짜별로 순차 크롤링 실행하는 메인 함수
    """
    logger.info("🚀 LH 백필 크롤링 시작")
    
    # 날짜 범위 생성
    start_date = BACKFILL_CONFIG['start_date']
    end_date = BACKFILL_CONFIG['end_date']
    skip_weekends = BACKFILL_CONFIG['skip_weekends']
    
    # 처리할 날짜 목록 생성
    target_dates = []
    current_date = start_date
    
    while current_date <= end_date:
        # 주말 제외 옵션
        if skip_weekends and current_date.weekday() >= 5:
            current_date += timedelta(days=1)
            continue
        target_dates.append(current_date)
        current_date += timedelta(days=1)
    
    logger.info(f"📅 처리 대상: {start_date} ~ {end_date}")
    logger.info(f"📊 총 {len(target_dates)}일 처리 예정")
    if skip_weekends:
        logger.info("📅 주말 제외됨")
    
    # MySQL 연결 확인
    mysql_hook = MySqlHook(mysql_conn_id='notices_db')
    
    # 통계 변수
    total_processed = 0
    total_errors = 0
    daily_results = []
    
    # 날짜별 크롤링 실행
    for idx, target_date in enumerate(target_dates):
        day_num = idx + 1
        logger.info(f"📆 [{day_num}/{len(target_dates)}] {target_date} 크롤링 시작")
        
        try:
            # 해당 날짜 크롤링
            start_time = time.time()
            
            notices_data = collect_lh_notices(
                list_url=LH_CONFIG['list_url'],
                headers=LH_CONFIG['headers'],
                target_date=target_date
            )
            
            processing_time = time.time() - start_time
            
            if notices_data:
                logger.info(f"✅ {target_date}: {len(notices_data)}개 공고 발견 ({processing_time:.1f}초)")
                
                # 데이터 분류
                csv_file_path = f"/opt/airflow/downloads/backfill_dag/mysql_failed_records/{target_date.strftime('%Y%m%d')}.csv"
                db_notices, csv_notices = classify_notices_by_completeness(notices_data, csv_file_path)
                
                # DB 저장
                saved_count = 0
                if db_notices:
                    saved_count = save_notices_to_db(db_notices, mysql_hook, target_date)
                    logger.info(f"💾 {target_date}: DB에 {saved_count}개 저장")
                
                if csv_notices:
                    logger.info(f"📝 {target_date}: CSV에 {len(csv_notices)}개 저장")
                
                total_processed += saved_count
                
                daily_results.append({
                    'date': target_date.strftime('%Y-%m-%d'),
                    'crawled': len(notices_data),
                    'db_saved': saved_count,
                    'csv_saved': len(csv_notices),
                    'processing_time': round(processing_time, 1),
                    'status': 'success'
                })
                
            else:
                logger.info(f"📭 {target_date}: 공고 없음 ({processing_time:.1f}초)")
                daily_results.append({
                    'date': target_date.strftime('%Y-%m-%d'),
                    'crawled': 0,
                    'db_saved': 0,
                    'csv_saved': 0,
                    'processing_time': round(processing_time, 1),
                    'status': 'no_data'
                })
            
            # 중간 보고 (30일마다)
            if day_num % BACKFILL_CONFIG['batch_size'] == 0:
                logger.info(f"📊 중간 보고 ({day_num}/{len(target_dates)}일 완료)")
                logger.info(f"   - 누적 DB 저장: {total_processed}건")
                logger.info(f"   - 누적 오류: {total_errors}건")
                logger.info(f"   - 진행률: {day_num/len(target_dates)*100:.1f}%")
            
            # 날짜 간 대기
            if idx < len(target_dates) - 1:
                time.sleep(BACKFILL_CONFIG['delay_between_dates'])
                
        except Exception as e:
            logger.error(f"❌ {target_date} 크롤링 실패: {str(e)}")
            total_errors += 1
            
            daily_results.append({
                'date': target_date.strftime('%Y-%m-%d'),
                'error': str(e),
                'status': 'error'
            })
            
            # 에러 시 더 긴 대기
            time.sleep(10)
    
    # 최종 결과
    success_rate = (total_processed / (total_processed + total_errors) * 100) if (total_processed + total_errors) > 0 else 0
    
    result = {
        'total_dates': len(target_dates),
        'total_processed': total_processed,
        'total_errors': total_errors,
        'success_rate': success_rate,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
        'daily_results': daily_results
    }
    
    logger.info(f"🎉 백필 완료!")
    logger.info(f"📊 최종 결과:")
    logger.info(f"   - 처리 기간: {start_date} ~ {end_date}")
    logger.info(f"   - 처리 날짜: {len(target_dates)}일")
    logger.info(f"   - DB 저장: {total_processed}건")
    logger.info(f"   - 오류 발생: {total_errors}건")
    logger.info(f"   - 성공률: {success_rate:.1f}%")
    
    return result

def save_notices_to_db(notices: List[Dict], mysql_hook: MySqlHook, target_date) -> int:
    """
    공고 데이터를 MySQL DB에 저장
    """
    if not notices:
        return 0
    
    saved_count = 0
    conn = None
    cursor = None
    
    try:
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        conn.autocommit = False
        
        for notice in notices:
            try:
                # UpsertNotice 프로시저 실행
                params = (
                    notice['notice_number'],
                    notice['notice_title'],
                    notice['post_date'],
                    notice.get('application_end_date'),
                    notice.get('document_start_date'),
                    notice.get('document_end_date'),
                    notice.get('contract_start_date'),
                    notice.get('contract_end_date'),
                    notice.get('winning_date'),
                    notice.get('move_in_date'),
                    notice.get('location'),
                    notice.get('is_correction', False),
                    json.dumps(notice.get('supply_type', []), ensure_ascii=False),
                    json.dumps(notice.get('house_types', []), ensure_ascii=False)
                )
                
                cursor.callproc('UpsertNotice', params)
                conn.commit()
                
                # 저장 확인
                verify_sql = "SELECT COUNT(*) FROM notices WHERE notice_number = %s"
                cursor.execute(verify_sql, (notice['notice_number'],))
                if cursor.fetchone()[0] > 0:
                    saved_count += 1
                
            except Exception as e:
                logger.error(f"❌ 공고 저장 실패 {notice['notice_number']}: {str(e)}")
                conn.rollback()
                continue
        
        # 상태 업데이트
        try:
            cursor.callproc('UpdateAllNoticeStatuses')
            conn.commit()
        except Exception as e:
            logger.warning(f"⚠️ 상태 업데이트 실패: {str(e)}")
        
    except Exception as e:
        logger.error(f"❌ DB 저장 중 오류: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return saved_count

# Task 정의
start_task = EmptyOperator(
    task_id='start',
    dag=backfill_dag
)

crawl_task = PythonOperator(
    task_id='run_backfill',
    python_callable=run_backfill_crawling,
    dag=backfill_dag
)

end_task = EmptyOperator(
    task_id='end',
    dag=backfill_dag
)

# Task 의존성
start_task >> crawl_task >> end_task

# DAG 문서화
backfill_dag.doc_md = """
## LH 공고문 간단 백필 DAG

기존 크롤링 함수의 target_date만 바꿔가며 과거 데이터를 수집하는 간단한 백필 DAG

### 🎯 핵심 아이디어:
- 복잡한 배치 처리 대신 **날짜별 순차 크롤링**
- 기존 `collect_lh_notices(target_date=날짜)` 함수 그대로 활용
- 설정만 바꾸면 즉시 실행 가능

### ⚙️ 설정 변경:
```python
BACKFILL_CONFIG = {
    'start_date': datetime(2024, 6, 1).date(),    # 시작일
    'end_date': datetime(2024, 12, 31).date(),    # 종료일
    'skip_weekends': True,                        # 주말 제외
    'delay_between_dates': 5,                     # 날짜 간 대기(초)
}
```

### 🚀 실행:
```bash
airflow dags trigger LH_Notice_Simple_Backfill
```

### 📊 예상 시간:
- **1년 (평일만)**: 약 260일 × 평균 30초 = 2-3시간
- **6개월**: 약 130일 × 평균 30초 = 1-2시간
- **1개월**: 약 22일 × 평균 30초 = 10-15분

### 🛡️ 안전장치:
- 날짜별 3초 대기 (서버 부하 방지)
- 에러 시 자동 스킵 & 10초 대기
- 30일마다 중간 진행률 보고
- 주말 자동 제외 옵션

### 💡 장점:
- **단순함**: 복잡한 배치 로직 없음
- **안정성**: 기존 검증된 크롤링 함수 사용  
- **유연성**: 날짜 범위 쉽게 조정
- **모니터링**: 실시간 진행률 확인
"""