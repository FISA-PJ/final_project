# -*- coding: utf-8 -*-

"""
LH 공고문 크롤링 및 MySQL 저장 DAG (리소스 모니터링 포함)

이 DAG는 다음과 같은 주요 기능을 수행합니다:
1. LH 공고 웹사이트에서 새로운 공고문을 크롤링
2. 주소 정보 유무에 따라 데이터 분리
3. 주소가 있는 공고는 MySQL DB에 저장 (프로시저 사용)
4. 주소가 없는 공고는 CSV 파일로 저장
5. 공고 상태 자동 업데이트 (접수중/접수마감)
6. 처리 결과 모니터링 및 로깅
7. **시스템 리소스 및 네트워크 사용량 모니터링**

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
import json
import psutil
import time
from typing import Optional
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

# 크롤러 모듈 임포트
from plugins.crawlers.lh_crawler_for_mysql import (
    collect_lh_notices,
    classify_notices_by_completeness,
)

# 로깅 설정
logger = logging.getLogger(__name__)

# DAG 기본 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=5),
}

# DAG 정의
dag = DAG(
    dag_id="LH_Notice_To_Mysql",
    default_args=default_args,
    description="LH 공고문 크롤링 및 저장 DAG (리소스 모니터링 포함)",
    schedule="00 3 * * *",
    start_date=datetime(2025, 5, 27),
    max_active_runs=1,
    catchup=True,
    tags=["crawler", "LH", "notices", "monitoring"]
)

# 크롤링 설정
LH_CONFIG = {
    "list_url": "https://apply.lh.or.kr/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch",
    "headers": {
        "User-Agent": "Mozilla/5.0",
        "Accept-Encoding": "gzip, deflate",  # 압축 활성화
        "Connection": "keep-alive",  # 연결 재사용
    },
}


def monitor_resources_before(**context):
    """
    크롤링 시작 전 시스템 리소스 모니터링

    Returns:
        None (XCom으로 데이터 전달)

    Note:
        - 시스템 리소스 현황 로깅
        - 네트워크 사용량 기준점 설정
        - 크롤링 후 비교를 위한 데이터 저장
    """
    logger.info("🔍 크롤링 시작 전 시스템 리소스 체크")

    # 시스템 리소스 체크
    cpu_percent = psutil.cpu_percent(interval=1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    network_before = psutil.net_io_counters()

    # 현재 시스템 상태 로깅
    logger.info(f"📊 시작 전 시스템 리소스 현황:")
    logger.info(f"- CPU 사용률: {cpu_percent}%")
    logger.info(f"- 메모리 사용률: {memory.percent}%")
    logger.info(f"- 사용 가능 메모리: {memory.available / 1024 / 1024:.0f} MB")
    logger.info(f"- 디스크 사용률: {disk.percent}%")
    logger.info(f"- 사용 가능 디스크: {disk.free / 1024 / 1024 / 1024:.1f} GB")

    # 네트워크 기준점 저장 (크롤링 후 비교용)
    baseline_data = {
        "timestamp": time.time(),
        "cpu_percent": cpu_percent,
        "memory_used": memory.used,
        "memory_percent": memory.percent,
        "disk_percent": disk.percent,
        "network_bytes_sent": network_before.bytes_sent,
        "network_bytes_recv": network_before.bytes_recv,
        "network_packets_sent": network_before.packets_sent,
        "network_packets_recv": network_before.packets_recv,
    }

    # XCom으로 기준 데이터 전달
    context["ti"].xcom_push(key="resource_baseline", value=baseline_data)
    logger.info("✅ 리소스 기준점 설정 완료")


def monitor_resources_after(**context):
    """
    크롤링 완료 후 리소스 사용량 계산 및 분석

    Args:
        **context: Airflow context 변수들

    Returns:
        dict: 리소스 사용량 분석 결과

    Note:
        - 크롤링 전후 리소스 사용량 비교
        - 네트워크 효율성 분석
        - 성능 지표 계산
    """
    logger.info("📊 크롤링 완료 후 리소스 사용량 분석 시작")

    # 크롤링 전 기준 데이터 가져오기
    baseline_data = context["ti"].xcom_pull(
        task_ids="monitor_resources_before", key="resource_baseline"
    )

    # 크롤링 결과 데이터 가져오기
    crawl_result = context["ti"].xcom_pull(task_ids="process_and_save_notices")

    if not baseline_data:
        logger.warning("⚠️ 기준 데이터를 찾을 수 없습니다.")
        return None

    # 현재 시스템 상태 측정
    current_time = time.time()
    current_cpu = psutil.cpu_percent(interval=1)
    current_memory = psutil.virtual_memory()
    current_disk = psutil.disk_usage("/")
    current_network = psutil.net_io_counters()

    # 사용량 계산
    duration = current_time - baseline_data["timestamp"]

    # 네트워크 사용량 (MB 단위)
    network_sent_mb = (
        (current_network.bytes_sent - baseline_data["network_bytes_sent"]) / 1024 / 1024
    )
    network_recv_mb = (
        (current_network.bytes_recv - baseline_data["network_bytes_recv"]) / 1024 / 1024
    )
    total_network_mb = network_sent_mb + network_recv_mb

    # 패킷 수
    packets_sent = current_network.packets_sent - baseline_data["network_packets_sent"]
    packets_recv = current_network.packets_recv - baseline_data["network_packets_recv"]
    total_packets = packets_sent + packets_recv

    # 메모리 사용량 변화 (MB 단위)
    memory_diff_mb = (current_memory.used - baseline_data["memory_used"]) / 1024 / 1024

    # CPU 평균 사용률
    avg_cpu = (current_cpu + baseline_data["cpu_percent"]) / 2

    # 성능 지표 계산
    crawled_count = crawl_result.get("total_crawled", 0) if crawl_result else 0
    mb_per_notice = total_network_mb / crawled_count if crawled_count > 0 else 0
    notices_per_minute = crawled_count / (duration / 60) if duration > 0 else 0
    network_speed_mbps = total_network_mb / (duration / 60) if duration > 0 else 0

    # 분석 결과 구성
    analysis_result = {
        "duration_seconds": round(duration, 1),
        "duration_minutes": round(duration / 60, 1),
        "network_sent_mb": round(network_sent_mb, 2),
        "network_recv_mb": round(network_recv_mb, 2),
        "total_network_mb": round(total_network_mb, 2),
        "packets_sent": packets_sent,
        "packets_recv": packets_recv,
        "total_packets": total_packets,
        "memory_diff_mb": round(memory_diff_mb, 1),
        "avg_cpu_percent": round(avg_cpu, 1),
        "current_cpu_percent": round(current_cpu, 1),
        "current_memory_percent": round(current_memory.percent, 1),
        "current_disk_percent": round(current_disk.percent, 1),
        "crawled_notices": crawled_count,
        "mb_per_notice": round(mb_per_notice, 3),
        "notices_per_minute": round(notices_per_minute, 1),
        "network_speed_mbps": round(network_speed_mbps, 2),
    }

    # 상세 로깅
    logger.info(f"📊 크롤링 리소스 사용량 분석 결과:")
    logger.info(
        f"⏱️  실행 시간: {analysis_result['duration_minutes']}분 ({analysis_result['duration_seconds']}초)"
    )
    logger.info(f"🌐 네트워크 사용량:")
    logger.info(f"   - 송신: {analysis_result['network_sent_mb']} MB")
    logger.info(f"   - 수신: {analysis_result['network_recv_mb']} MB")
    logger.info(f"   - 총 사용량: {analysis_result['total_network_mb']} MB")
    logger.info(f"   - 평균 속도: {analysis_result['network_speed_mbps']} MB/분")
    logger.info(f"📦 패킷 정보:")
    logger.info(f"   - 송신 패킷: {analysis_result['packets_sent']:,}개")
    logger.info(f"   - 수신 패킷: {analysis_result['packets_recv']:,}개")
    logger.info(f"💻 시스템 리소스:")
    logger.info(f"   - 평균 CPU: {analysis_result['avg_cpu_percent']}%")
    logger.info(f"   - 메모리 변화: {analysis_result['memory_diff_mb']:+.1f} MB")
    logger.info(f"📈 성능 지표:")
    logger.info(f"   - 처리한 공고: {analysis_result['crawled_notices']}개")
    logger.info(f"   - MB/공고: {analysis_result['mb_per_notice']}")
    logger.info(f"   - 공고/분: {analysis_result['notices_per_minute']}")

    # 효율성 평가
    if analysis_result["mb_per_notice"] > 1.0:
        logger.warning(
            f"⚠️ 공고당 네트워크 사용량이 높습니다 ({analysis_result['mb_per_notice']} MB/공고)"
        )
    elif analysis_result["mb_per_notice"] < 0.1:
        logger.info(
            f"✅ 효율적인 네트워크 사용량입니다 ({analysis_result['mb_per_notice']} MB/공고)"
        )

    # XCom으로 결과 전달
    context["ti"].xcom_push(key="resource_analysis", value=analysis_result)

    return analysis_result


def crawl_lh_notices_task(**context):
    """
    LH 공고문 크롤링 Task
    """
    execution_date = context.get("ds")
    execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
    print(f"🔄 실행 날짜: {execution_date}")

    try:
        notices_data = collect_lh_notices(
            list_url=LH_CONFIG["list_url"],
            headers=LH_CONFIG["headers"],
            target_date=execution_date,
        )
        return notices_data

    except Exception as e:
        logger.error(f"❌ 크롤링 Task 중 실패: {str(e)}")
        raise


def process_and_save_notices_task(**context):
    """
    크롤링 데이터 처리 및 저장 Task
    """
    logger.info("크롤링 데이터 처리 및 저장 시작")

    # 이전 Task에서 크롤링한 데이터 가져오기
    notices_data = context["ti"].xcom_pull(task_ids="crawl_lh_notices")

    if not notices_data:
        logger.info("크롤링된 데이터가 없습니다.")
        return {"total_crawled": 0, "db_saved": 0, "csv_saved": 0, "errors": 0}

    # CSV 파일 경로 설정
    download_dir = "/opt/airflow/downloads/normal_dag/mysql_failed_records"
    today_str = context["ds"].replace("-", "")
    csv_file_path = f"{download_dir}/{today_str}.csv"

    # 데이터 완성도에 따라 분류
    db_notices, csv_notices = classify_notices_by_completeness(
        notices_data, csv_file_path
    )
    logger.info(
        f"데이터 분리 완료 - DB용: {len(db_notices)}개, CSV용: {len(csv_notices)}개"
    )

    try:
        # MySQL 연결 설정
        mysql_hook = MySqlHook(mysql_conn_id="notices_db")
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        # 연결 확인
        logger.info("MySQL 연결 확인")
        cursor.execute("SELECT 1")

        # 연결 정보 로깅
        conn_info = mysql_hook.get_connection("notices_db")
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

    # DB에 공고 저장
    for notice in db_notices:
        try:
            # UpsertNotice 프로시저 호출을 위한 파라미터 준비
            params = (
                notice["notice_number"],
                notice["notice_title"],
                notice["post_date"],
                notice.get("application_end_date"),
                notice.get("document_start_date"),
                notice.get("document_end_date"),
                notice.get("contract_start_date"),
                notice.get("contract_end_date"),
                notice.get("winning_date"),
                notice.get("move_in_date"),
                notice.get("location"),
                notice.get("is_correction", False),
                json.dumps(notice.get("supply_type", []), ensure_ascii=False),
                json.dumps(notice.get("house_types", []), ensure_ascii=False),
            )

            logger.info(
                f"🔄 프로시저 실행 시작 - UpsertNotice for {notice['notice_number']}"
            )

            # 프로시저 실행 전에 autocommit 비활성화
            conn.autocommit = False

            # 프로시저 실행
            cursor.callproc("UpsertNotice", params)

            # 커밋
            conn.commit()
            logger.info("✅ commit 완료")

            # 데이터 확인
            verify_sql = "SELECT * FROM notices WHERE notice_number = %s"
            cursor.execute(verify_sql, (notice["notice_number"],))
            saved_notice = cursor.fetchone()

            if saved_notice:
                logger.info(
                    f"✅ DB 저장 확인 - 공고번호 {notice['notice_number']}가 실제로 저장되었습니다."
                )
                if notice.get("is_correction"):
                    logger.info(f"🔄 정정공고 처리 완료: {notice['notice_number']}")
                db_saved_count += 1
            else:
                logger.error(
                    f"❌ 오류: 공고번호 {notice['notice_number']}가 DB에 저장되지 않았습니다!"
                )
                error_count += 1

        except Exception as e:
            logger.error(f"❌ 'UpsertNotice' 프로시저 실행 중 오류 발생: {str(e)}")
            conn.rollback()
            error_count += 1
            continue

    try:
        # 모든 공고의 상태 업데이트
        cursor.callproc("UpdateAllNoticeStatuses")
        conn.commit()
        logger.info("✅ 전체 공고 상태(결과발표/접수중/접수마감) 업데이트 완료")
    except Exception as e:
        logger.error(f"❌ 공고 상태(결과발표/접수중/접수마감) 업데이트 실패: {str(e)}")
        error_count += 1
        conn.rollback()

    # 연결 종료
    cursor.close()
    conn.close()

    # 처리 결과 반환
    result = {
        "total_crawled": len(notices_data),
        "db_saved": db_saved_count,
        "csv_saved": len(csv_notices),
        "errors": error_count,
        "csv_file": csv_file_path,
    }

    logger.info(f"📊 처리 완료 통계:")
    logger.info(f"- 전체 크롤링: {len(notices_data)}건")
    logger.info(f"- DB 저장 성공: {db_saved_count}건")
    logger.info(f"- CSV 저장: {len(csv_notices)}건")
    logger.info(f"- 오류 발생: {error_count}건")

    return result


def log_crawl_summary(**context):
    """
    크롤링 결과 종합 요약 (처리 결과 + 리소스 사용량 통합)

    Args:
        **context: Airflow context 변수들

    Note:
        - 전체 처리 결과 통계 출력
        - 리소스 사용량 및 효율성 분석
        - 성능 지표 및 개선 제안
        - 시스템 상태 종합 진단
    """
    # 크롤링 결과 데이터
    result = context["ti"].xcom_pull(task_ids="process_and_save_notices")

    # 리소스 분석 데이터
    resource_analysis = context["ti"].xcom_pull(
        task_ids="monitor_resources_after", key="resource_analysis"
    )

    # 현재 시스템 상태 (요약 시점)
    current_cpu = psutil.cpu_percent(interval=1)
    current_memory = psutil.virtual_memory()
    current_disk = psutil.disk_usage("/")

    if result:
        success_rate = (
            ((result["db_saved"] + result["csv_saved"]) / result["total_crawled"] * 100)
            if result["total_crawled"] > 0
            else 0
        )

        # 종합 요약 생성
        summary = f"""
        ========================================
        🏠 LH 크롤링 완료 종합 요약
        ========================================
        ⏰ 실행일시: {context['ds']}
        🔖 DAG Run ID: {context['dag_run'].run_id}
        
        📊 처리 결과:
        - 총 크롤링: {result['total_crawled']}건
        - DB 저장 (주소 있음): {result['db_saved']}건
        - CSV 저장 (주소 없음): {result['csv_saved']}건
        - 오류 발생: {result['errors']}건
        - ✅ 성공률: {success_rate:.1f}%
        """

        # 리소스 분석 정보 추가
        if resource_analysis:
            summary += f"""
        🔍 리소스 사용량 분석:
        - ⏱️  총 실행시간: {resource_analysis['duration_minutes']}분
        - 🌐 네트워크 사용량: {resource_analysis['total_network_mb']} MB
           └─ 송신: {resource_analysis['network_sent_mb']} MB
           └─ 수신: {resource_analysis['network_recv_mb']} MB
        - 📦 총 패킷: {resource_analysis['total_packets']:,}개
        - 💻 평균 CPU: {resource_analysis['avg_cpu_percent']}%
        - 🧠 메모리 변화: {resource_analysis['memory_diff_mb']:+.1f} MB
        
        📈 성능 지표:
        - 공고당 네트워크 사용량: {resource_analysis['mb_per_notice']} MB
        - 처리 속도: {resource_analysis['notices_per_minute']} 공고/분
        - 네트워크 속도: {resource_analysis['network_speed_mbps']} MB/분
        """

            # 성능 평가 및 제안
            summary += f"""
        💡 성능 평가:
        """

            # 네트워크 효율성 평가
            if resource_analysis["mb_per_notice"] > 1.0:
                summary += f"        - ⚠️  네트워크 사용량 높음 ({resource_analysis['mb_per_notice']} MB/공고)\n"
                summary += f"        - 💡 제안: 요청 간격 추가, 배치 크기 조정 검토\n"
            elif resource_analysis["mb_per_notice"] < 0.1:
                summary += f"        - ✅ 효율적인 네트워크 사용량 ({resource_analysis['mb_per_notice']} MB/공고)\n"
            else:
                summary += f"        - 🟡 보통 수준의 네트워크 사용량 ({resource_analysis['mb_per_notice']} MB/공고)\n"

            # 처리 속도 평가
            if resource_analysis["notices_per_minute"] > 10:
                summary += f"        - ✅ 빠른 처리 속도 ({resource_analysis['notices_per_minute']} 공고/분)\n"
            elif resource_analysis["notices_per_minute"] < 3:
                summary += f"        - ⚠️  느린 처리 속도 ({resource_analysis['notices_per_minute']} 공고/분)\n"
                summary += (
                    f"        - 💡 제안: 네트워크 연결 상태 확인, 병렬 처리 검토\n"
                )
            else:
                summary += f"        - 🟡 보통 처리 속도 ({resource_analysis['notices_per_minute']} 공고/분)\n"

            # 실행 시간 평가
            if resource_analysis["duration_minutes"] > 60:
                summary += f"        - ⚠️  긴 실행 시간 ({resource_analysis['duration_minutes']}분)\n"
                summary += (
                    f"        - 💡 제안: 타임아웃 설정 확인, 크롤링 최적화 필요\n"
                )
            else:
                summary += f"        - ✅ 적절한 실행 시간 ({resource_analysis['duration_minutes']}분)\n"

        # 현재 시스템 상태 추가
        summary += f"""
        💻 현재 시스템 상태:
        - CPU 사용률: {current_cpu}%
        - 메모리 사용률: {current_memory.percent}%
        - 디스크 사용률: {current_disk.percent}%
        - 사용 가능 메모리: {current_memory.available / 1024 / 1024:.0f} MB
        - 사용 가능 디스크: {current_disk.free / 1024 / 1024 / 1024:.1f} GB
        
        📁 파일 위치:
        - CSV 파일: {result.get('csv_file', 'N/A')}
        
        ========================================
        """

        # 종합 평가 및 권장사항
        summary += f"""
        🎯 종합 평가:
        """

        if success_rate >= 95:
            summary += f"        - ✅ 우수: 성공률 {success_rate:.1f}%로 매우 안정적\n"
        elif success_rate >= 80:
            summary += (
                f"        - 🟡 양호: 성공률 {success_rate:.1f}%로 대체로 안정적\n"
            )
        else:
            summary += f"        - ⚠️  주의: 성공률 {success_rate:.1f}%로 개선 필요\n"
            summary += f"        - 💡 권장: 오류 로그 분석 및 예외 처리 강화\n"

        # 시스템 리소스 상태 평가
        if current_cpu > 80:
            summary += f"        - ⚠️  CPU 사용률 높음 ({current_cpu}%)\n"
        if current_memory.percent > 80:
            summary += f"        - ⚠️  메모리 사용률 높음 ({current_memory.percent}%)\n"
        if current_disk.percent > 80:
            summary += f"        - ⚠️  디스크 사용률 높음 ({current_disk.percent}%)\n"

        summary += f"""
        ========================================
        """

        logger.info(summary)

    else:
        logger.warning("⚠️ 크롤링 결과 데이터를 찾을 수 없습니다.")


# Task 정의
start_task = EmptyOperator(task_id="start", dag=dag)

monitor_before_task = PythonOperator(
    task_id="monitor_resources_before",
    python_callable=monitor_resources_before,
    dag=dag,
)

crawl_task = PythonOperator(
    task_id="crawl_lh_notices", python_callable=crawl_lh_notices_task, dag=dag
)

save_task = PythonOperator(
    task_id="process_and_save_notices",
    python_callable=process_and_save_notices_task,
    dag=dag,
)

monitor_after_task = PythonOperator(
    task_id="monitor_resources_after", python_callable=monitor_resources_after, dag=dag
)

summary_task = PythonOperator(
    task_id="log_crawl_summary",
    python_callable=log_crawl_summary,
    trigger_rule="all_done",  # 이전 태스크 성공/실패 관계없이 실행
    dag=dag,
)

end_task = EmptyOperator(task_id="end", dag=dag)

# Task 의존성 설정 (리소스 모니터링 포함)
(
    start_task
    >> monitor_before_task
    >> crawl_task
    >> save_task
    >> monitor_after_task
    >> summary_task
    >> end_task
)

# DAG 문서화
dag.doc_md = """
## LH 공고문 크롤링 DAG (리소스 모니터링 포함)

이 DAG는 LH(한국토지주택공사) 공고문을 크롤링하여 처리하며, 시스템 리소스 사용량을 모니터링합니다.

### 주요 기능:
1. **사전 모니터링**: 크롤링 시작 전 시스템 리소스 현황 체크
2. **크롤링**: LH 공고문 사이트에서 최신 공고 수집
3. **데이터 처리**: 주소 유무에 따라 DB/CSV 분리 저장
4. **사후 모니터링**: 크롤링 완료 후 리소스 사용량 분석
5. **종합 요약**: 처리 결과 + 리소스 분석 통합 리포트

### 모니터링 지표:
- **네트워크 사용량**: 송신/수신 데이터량, 패킷 수, 속도
- **시스템 리소스**: CPU, 메모리, 디스크 사용률
- **성능 지표**: 공고당 네트워크 사용량, 처리 속도, 효율성
- **품질 지표**: 성공률, 오류율, 안정성

### 성능 최적화:
- 네트워크 사용량 추적으로 비효율적인 크롤링 패턴 감지
- 리소스 사용량 분석을 통한 시스템 부하 관리
- 처리 속도 모니터링으로 병목구간 식별

### 출력:
- **DB**: 주소 정보가 있는 공고 (notices 테이블)
- **CSV**: 주소 정보가 없는 공고
- **로그**: 상세한 리소스 사용량 및 성능 분석 리포트
"""
