from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from plugins.crawlers.lh_crawler_for_elastic import collect_lh_file_urls_and_pdf # 크롤링 모듈 import
from plugins.crawlers.lh_crawler_for_elastic import CommonConfig

from elasticsearch import Elasticsearch

import json
import shutil
import pendulum
import logging
import os
import time
import psutil

# 타임존 설정 (Asia/Seoul 기준)
local_tz = pendulum.timezone("Asia/Seoul")

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2025, 5, 14, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(seconds=10), # 재시도 간격
    'execution_timeout':timedelta(hours=72), # 실행 시간 제한
    'email_on_failure': False
    # 'email': ['admin@example.com']
}

logger = logging.getLogger('airflow.task')

# DAG 정의
with DAG(
    dag_id='PDF_CRAWLER_TO_ES',
    default_args=default_args,
    schedule="@daily",
    catchup=True,
    max_active_runs=1,  # 동시 실행 방지
    max_active_tasks=4,  # concurrency 대신 max_active_tasks 사용
    tags=['pdf', 'crawler', 'elasticsearch'],
    doc_md="""
    # PDF 크롤러 및 Elasticsearch 적재 DAG
    
    ## 개요
    LH 공고 PDF를 크롤링하고 Elasticsearch에 적재하는 DAG
    
    ## 태스크 구성
    1. **check_system_resources**: 시스템 리소스 사전 체크
    2. **crawl_pdfs**: PDF 파일 크롤링
    3. **process_to_es**: PDF 처리 및 ES 적재
    4. **organize_files**: 파일 정리 및 분류
    5. **check_system_resources_after**: 시스템 리소스 사후 체크
    
    ## 주의사항
    - 디스크 공간 최소 5GB 필요
    - 메모리 사용률 85% 이상 시 주의
    """
) as dag:
    # ==================================================================
    # TASK 0: 크롤링 시작 전 시스템 리소스 체크
    # ==================================================================
    def check_system_resources(**context):
        """크롤링 시작 전 시스템 리소스 체크"""
        from airflow.exceptions import AirflowException
        
        logger.info("크롤링 시작 전 시스템 리소스 체크")
        
        # 리소스 임계값 설정
        RESOURCE_THRESHOLDS = {
            'cpu_percent': 80,
            'memory_percent': 85,
            'disk_percent': 90,
            'min_disk_gb': 5
        }
        
        # 시스템 리소스 체크
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network_before = psutil.net_io_counters()
        
        # 리소스 부족 시 경고 또는 실패 처리
        warnings = []
        
        if cpu_percent > RESOURCE_THRESHOLDS['cpu_percent']:
            warnings.append(f"CPU 사용률이 높습니다: {cpu_percent}%")
        
        if memory.percent > RESOURCE_THRESHOLDS['memory_percent']:
            warnings.append(f"메모리 사용률이 높습니다: {memory.percent}%")
        
        if disk.percent > RESOURCE_THRESHOLDS['disk_percent']:
            warnings.append(f"디스크 사용률이 높습니다: {disk.percent}%")
        
        if disk.free / 1024 / 1024 / 1024 < RESOURCE_THRESHOLDS['min_disk_gb']:
            warnings.append(f"사용 가능한 디스크 공간이 부족합니다: {disk.free / 1024 / 1024 / 1024:.1f} GB")
        
        if warnings:
            for warning in warnings:
                logger.warning(f"⚠️ {warning}")
            
            # 심각한 경우 태스크 실패 처리
            if disk.free / 1024 / 1024 / 1024 < 1:  # 1GB 미만
                raise AirflowException("디스크 공간이 부족하여 크롤링을 시작할 수 없습니다")
        
        # 현재 시스템 상태 로깅
        logger.info(f"📊 시작 전 시스템 리소스 현황:")
        logger.info(f"- CPU 사용률: {cpu_percent}%")
        logger.info(f"- 메모리 사용률: {memory.percent}%")
        logger.info(f"- 사용 가능 메모리: {memory.available / 1024 / 1024:.0f} MB")
        logger.info(f"- 디스크 사용률: {disk.percent}%")
        logger.info(f"- 사용 가능 디스크: {disk.free / 1024 / 1024 / 1024:.1f} GB")

        # 네트워크 기준점 저장 (크롤링 후 비교용)
        baseline_data = {
            'timestamp': time.time(),
            'cpu_percent': cpu_percent,
            'memory_used': memory.used,
            'memory_percent': memory.percent,
            'disk_percent': disk.percent,
            'network_bytes_sent': network_before.bytes_sent,
            'network_bytes_recv': network_before.bytes_recv,
            'network_packets_sent': network_before.packets_sent,
            'network_packets_recv': network_before.packets_recv,
            'warnings': warnings  # 경고 사항도 저장
        }
        
        # XCom으로 기준 데이터 전달
        context['ti'].xcom_push(key='resource_baseline', value=baseline_data)
        logger.info("✅ 리소스 기준점 설정 완료")

    check_system_resources_task = PythonOperator(
        task_id='check_system_resources',
        python_callable=check_system_resources
    )

    # ==================================================================
    # TASK 1: PDF 크롤링 (크롤링 모듈 실행)
    # ==================================================================
    def crawl_pdfs(**context):
        """외부 크롤링 스크립트 실행 및 실패 기록 관리"""
        
        task_logger = context.get('logger') or logger
        
        # 태스크 실행 시점에 디렉토리 생성
        try:
            CommonConfig.ensure_directories()
            task_logger.info("필요한 디렉토리 생성 완료")
        except Exception as e:
            task_logger.warning(f"디렉토리 생성 중 오류: {e}")
            # 최소한의 디렉토리는 직접 생성
            import os
            try:
                os.makedirs("/opt/airflow/downloads/normal_dag", exist_ok=True)
                task_logger.info("기본 다운로드 디렉토리 생성 완료")
            except Exception as dir_error:
                task_logger.error(f"기본 디렉토리 생성 실패: {dir_error}")
                raise
        execution_date = context.get('ds')
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
        
        try:
            task_logger.info("📥 PDF 크롤링 시작")
            
            # 크롤링 모듈 실행 (성공 시 파일 다운로드)
            crawl_result = collect_lh_file_urls_and_pdf(
                "https://apply.lh.or.kr",
                "https://apply.lh.or.kr/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch",
                "https://apply.lh.or.kr/lhapply/lhFile.do",
                "/opt/airflow/downloads/normal_dag",
                {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                execution_date
            ) 
            
            # XCom에 크롤링 기록 전달
            context['ti'].xcom_push(key='crawl_result', value=crawl_result)
            
        except Exception as e:
            task_logger.error(f"❌ 크롤링 모듈 실행 실패: {str(e)}")
            raise

    crawl_task = PythonOperator(
        task_id='crawl_pdfs',
        python_callable=crawl_pdfs
    )

    # ==================================================================
    # TASK 2: PDF 처리 및 ES 적재 (제공된 main.py 실행)
    # ==================================================================
    def process_to_es(**context):
        """PDF 처리 및 ES 적재 (main.py 실행)"""
        import subprocess
        from airflow.exceptions import AirflowException
        
        task_logger = context.get('logger') or logger
        
        # 이전 태스크에서 크롤링 기록 가져오기
        crawl_result = context['ti'].xcom_pull(key='crawl_result', task_ids='crawl_pdfs')
        
        if not crawl_result:
            logger.warning("⚠️ 수집된 공고가 없습니다 - 처리할 파일이 없음")
            return {'status': 'no_files', 'processed': 0}
        
        # crawl_result 리스트에서 pdf 파일명만 추출하여 리스트로 저장
        pdf_files = [item[1] for item in crawl_result]
        logger.info(f"📋 이전 태스크에서 수집된 공고 pdf 파일수: {len(pdf_files)}개")
        logger.info(f"📋 이전 태스크에서 수집된 공고 pdf 파일명: {pdf_files}")

        # JSON 형식으로 환경변수 전달
        os.environ['PROCESS_PDF_FILES'] = json.dumps(pdf_files, ensure_ascii=False)
        logger.info(f"📋 환경변수로 전달된 파일명 리스트 (JSON): {os.environ['PROCESS_PDF_FILES']}")
        
        # subprocess 실행 부분 개선
        try:
            # main.py 실행 (환경 변수 전달)
            task_logger.info("🔄 main.py 실행 시작")

            process = subprocess.Popen(
                ['python', '/opt/airflow/plugins/processors/main.py'],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # stderr를 stdout으로 리다이렉트
                text=True,
                bufsize=1,  # 라인 버퍼링
                env=os.environ.copy()  # 환경변수 명시적 전달
            )
            
            # 실시간 로그 출력
            for line in iter(process.stdout.readline, ''):
                if line:
                    task_logger.info(line.strip())
            
            return_code = process.wait()
            
            if return_code != 0:
                # 에러 메시지 개선
                error_msg = f"main.py 실행 실패 - 반환 코드: {return_code}"
                task_logger.error(f"❌ {error_msg}")
                
                # 실패 파일 목록 읽기
                execution_date = context.get('ds')
                execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
                failed_file_path = f"/opt/airflow/downloads/normal_dag/esupload_failed_records/{execution_date}_failed_files.json"
                
                failed_files = []
                try:
                    if os.path.exists(failed_file_path):
                        with open(failed_file_path, 'r') as f:
                            failed_data = json.load(f)
                            failed_files = failed_data.get('failed_files', [])
                            task_logger.info(f"📋 실패 파일 목록 로드: {len(failed_files)}개")
                    else:
                        task_logger.warning(f"⚠️ 실패 파일 목록 파일이 존재하지 않음: {failed_file_path}")
                        
                except Exception as read_error:
                    task_logger.error(f"❌ 실패 파일 목록 읽기 실패: {str(read_error)}")
                
                # XCom에 실패 파일 목록 저장
                context['ti'].xcom_push(key='failed_files', value=failed_files)
                
                raise AirflowException(error_msg)  # Airflow 전용 예외 사용
                
            task_logger.info("✅ main.py 실행 종료 - 적재 완료")
            
            # 성공 시에도 실패 파일 확인
            execution_date = context.get('ds')
            execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
            failed_file_path = f"/opt/airflow/downloads/normal_dag/esupload_failed_records/{execution_date}_failed_files.json"
            
            failed_files = []
            if os.path.exists(failed_file_path):
                with open(failed_file_path, 'r') as f:
                    failed_data = json.load(f)
                    failed_files = failed_data.get('failed_files', [])
                    
            context['ti'].xcom_push(key='failed_files', value=failed_files)
            
            return {'status': 'success', 'total': len(pdf_files), 'failed': len(failed_files)}
        
        except Exception as e:
            task_logger.error(f"❌ main.py 실행 중 오류 발생: {str(e)}")
            raise

    process_task = PythonOperator(
        task_id='process_to_es',
        python_callable=process_to_es
    )

    # ==================================================================
    # TASK 3: 파일 정리
    # ==================================================================
    def organize_files(**context):
        """성공/실패 파일 분류 및 오래된 파일 정리"""
        ti = context['ti']
        task_logger = context.get('logger') or logger
        
        # 설정
        RETENTION_DAYS = 7  # 파일 보관 기간
        
        # ES 적재 실패 파일 목록 조회
        failed_files = ti.xcom_pull(key='failed_files', task_ids='process_to_es') or []
        task_logger.info(f"📋 실패 파일 목록: {failed_files}")
        
        # 디렉토리 설정
        dirs = {
            'source': '/opt/airflow/downloads/normal_dag',
            'success': '/opt/airflow/downloads/normal_dag/es_success_pdf',
            'failed': '/opt/airflow/downloads/normal_dag/es_failed_pdf'
        }
        
        # 디렉토리 생성
        for dir_path in dirs.values():
            os.makedirs(dir_path, exist_ok=True)
        
        # 통계 초기화
        stats = {
            'moved_success': 0,
            'moved_failed': 0,
            'errors': 0,
            'cleaned_old': 0
        }
        
        # 1. 파일 이동
        try:
            # 실제 다운로드된 파일들이 있는 디렉토리에서 PDF 파일 찾기
            if os.path.exists(dirs['source']):
                for file_name in os.listdir(dirs['source']):
                    if not file_name.endswith('.pdf'):
                        continue
                        
                    source_path = os.path.join(dirs['source'], file_name)
                    
                    # 파일인지 확인 (디렉토리 제외)
                    if not os.path.isfile(source_path):
                        continue
                    
                    try:
                        # 실패 목록에 있는지 확인
                        if file_name in failed_files:
                            dest_dir = dirs['failed']
                            stats_key = 'moved_failed'
                        else:
                            dest_dir = dirs['success']
                            stats_key = 'moved_success'
                        
                        dest_path = os.path.join(dest_dir, file_name)
                        
                        # 중복 파일 처리
                        if os.path.exists(dest_path):
                            # 타임스탬프 추가
                            base_name, ext = os.path.splitext(file_name)
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            new_name = f"{base_name}_{timestamp}{ext}"
                            dest_path = os.path.join(dest_dir, new_name)
                        
                        # 파일 이동
                        shutil.move(source_path, dest_path)
                        stats[stats_key] += 1
                        task_logger.info(f"📦 {file_name} → {os.path.basename(dest_dir)}")
                        
                    except Exception as move_error:
                        stats['errors'] += 1
                        task_logger.error(f"❌ 파일 이동 실패 {file_name}: {move_error}")
            else:
                task_logger.warning(f"⚠️ 소스 디렉토리가 존재하지 않음: {dirs['source']}")
        
        except Exception as e:
            task_logger.error(f"❌ 파일 정리 중 오류: {e}")
            raise
        
        # 2. 오래된 파일 정리 (선택적)
        if RETENTION_DAYS > 0:
            cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
            
            for dir_name in ['success', 'failed']:
                dir_path = dirs[dir_name]
                try:
                    for file_name in os.listdir(dir_path):
                        file_path = os.path.join(dir_path, file_name)
                        if os.path.isfile(file_path):
                            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                            if file_mtime < cutoff_date:
                                os.remove(file_path)
                                stats['cleaned_old'] += 1
                                task_logger.info(f"🗑️ 오래된 파일 삭제: {file_name}")
                except Exception as e:
                    task_logger.error(f"오래된 파일 정리 중 오류: {e}")
        
        # 결과 요약
        task_logger.info(f"""
        📊 파일 정리 완료:
        - 성공 폴더로 이동: {stats['moved_success']}개
        - 실패 폴더로 이동: {stats['moved_failed']}개
        - 오류 발생: {stats['errors']}개
        - 오래된 파일 삭제: {stats['cleaned_old']}개
        """)
        
        # XCom으로 통계 전달
        context['ti'].xcom_push(key='organize_stats', value=stats)

    organize_task = PythonOperator(
        task_id='organize_files',
        python_callable=organize_files
    )

    # ==================================================================
    # TASK 4: 크롤링 및 ES 적재 후 시스템 리소스 체크
    # ==================================================================
    def check_system_resources_after(**context):
        """크롤링 및 ES 적재 후 시스템 리소스 체크"""
        logger.info("크롤링 및 ES 적재 후 시스템 리소스 체크")
        
        # 이전 태스크에서 기준 데이터 가져오기
        baseline_data = context['ti'].xcom_pull(key='resource_baseline', task_ids='check_system_resources')
        
        if not baseline_data:
            logger.warning("⚠️ 기준 데이터가 없습니다 - 리소스 분석 불가")
            return
        
        # 현재 시스템 상태 체크
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network_after = psutil.net_io_counters()
        
        # 리소스 변동 계산
        resource_diff = {
            'cpu_percent': cpu_percent - baseline_data['cpu_percent'],
            'memory_used': memory.used - baseline_data['memory_used'],
            'memory_percent': memory.percent - baseline_data['memory_percent'],
            'disk_percent': disk.percent - baseline_data['disk_percent'],
            'network_bytes_sent': network_after.bytes_sent - baseline_data['network_bytes_sent'],
            'network_bytes_recv': network_after.bytes_recv - baseline_data['network_bytes_recv'],
            'network_packets_sent': network_after.packets_sent - baseline_data['network_packets_sent'],
            'network_packets_recv': network_after.packets_recv - baseline_data['network_packets_recv']
        }
        
        # 리소스 변동 로깅
        logger.info(f"📊 시스템 리소스 변동 현황:")
        logger.info(f"- CPU 사용률 변동: {resource_diff['cpu_percent']}%")
        logger.info(f"- 메모리 사용률 변동: {resource_diff['memory_percent']}%")
        logger.info(f"- 디스크 사용률 변동: {resource_diff['disk_percent']}%")
        logger.info(f"- 네트워크 송신 변동: {resource_diff['network_bytes_sent']} bytes")
        logger.info(f"- 네트워크 수신 변동: {resource_diff['network_bytes_recv']} bytes")
        logger.info(f"- 네트워크 패킷 송신 변동: {resource_diff['network_packets_sent']} packets")
        logger.info(f"- 네트워크 패킷 수신 변동: {resource_diff['network_packets_recv']} packets")

        # 리소스 변동 평가
        if resource_diff['cpu_percent'] > 10:
            logger.warning(f"⚠️ CPU 사용률이 10% 이상 증가했습니다 ({resource_diff['cpu_percent']}%)")
        if resource_diff['memory_percent'] > 10:
            logger.warning(f"⚠️ 메모리 사용률이 10% 이상 증가했습니다 ({resource_diff['memory_percent']}%)")
        
        # XCom으로 리소스 변동 데이터 전달
        context['ti'].xcom_push(key='resource_diff', value=resource_diff)
        logger.info("✅ 리소스 변동 데이터 전달 완료")

    check_system_resources_after_task = PythonOperator(
        task_id='check_system_resources_after',
        python_callable=check_system_resources_after
    )

    # ==================================================================
    # TASK 의존성 설정
    # ==================================================================
    check_system_resources_task >> crawl_task >> process_task >> organize_task >> check_system_resources_after_task
