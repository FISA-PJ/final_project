# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, helpers
import os
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor

# PDFProcessor 클래스 임포트
from plugins.processors.pdf_processors import PDFProcessor

# Elasticsearch 클라이언트 가져오기 함수
def get_elasticsearch_client():
    """
    Elasticsearch 클라이언트를 생성하고 반환하는 함수
    
    Returns:
    --------
    Elasticsearch
        연결된 Elasticsearch 클라이언트 인스턴스
    """
    from airflow.hooks.base import BaseHook
    import logging

    # 여러 호스트 시도
    hosts = [
        "http://test-elasticsearch-ml:9200",  # Docker 서비스 이름
        "http://elasticsearch:9200",          # 일반적인 서비스 이름
        "http://localhost:9200"               # 로컬 테스트용
    ]
    
    # 먼저 Airflow Connection에서 시도
    try:
        conn = BaseHook.get_connection("elasticsearch_2")
        es_host = f"http://{conn.host}:{conn.port}"
        logging.info(f"🔍 Elasticsearch 연결 시도 (Airflow Connection): {es_host}")
        es = Elasticsearch(es_host, request_timeout=30)
        # 연결 테스트
        es.info()
        logging.info(f"✅ Elasticsearch 연결 성공: {es_host}")
        return es
    except Exception as e:
        logging.warning(f"⚠️ Airflow Connection 설정으로 Elasticsearch 연결 실패: {str(e)}")
        
    # 차례로 다른 호스트 시도
    for host in hosts:
        try:
            logging.info(f"🔄 Elasticsearch 연결 시도: {host}")
            es = Elasticsearch(host, request_timeout=30)
            # 연결 테스트
            es.info()
            logging.info(f"✅ Elasticsearch 연결 성공: {host}")
            return es
        except Exception as e:
            logging.warning(f"⚠️ Elasticsearch 연결 실패 ({host}): {str(e)}")
    
    # 모든 시도 실패 시 명확한 오류 메시지와 함께 예외 발생
    error_msg = "❌ 모든 Elasticsearch 연결 시도 실패"
    logging.error(error_msg)
    raise ConnectionError(error_msg)

# 수집된 PDF 파일 하나씩 처리하여 Elasticsearch에 업로드하는 함수
def process_single_pdf(
    processor: PDFProcessor,
    download_dir: str,
    safe_filename: str,
    meta: Dict[str, str]
) -> bool:
    """
    수집된 PDF 파일을 처리하여 Elasticsearch에 업로드하는 함수
    
    Parameters:
    -----------
    processor : PDFProcessor
        PDF 처리를 위한 프로세서 인스턴스
    download_dir : str
        PDF 파일이 저장된 디렉토리 경로
    safe_filename : str
        PDF 파일명
    meta : Dict[str, str]
        PDF 메타데이터
        
    Returns:
    --------
    bool
        처리 성공 여부
    """
    pdf_name = meta['filename']
    file_path = os.path.join(download_dir, safe_filename)
    
    try:
        # 파일 존재 확인
        if not os.path.exists(file_path):
            print(f"⚠️ [{pdf_name}] 파일이 존재하지 않습니다: {file_path}")
            return False
            
        print(f"🔍 [{pdf_name}] 처리 시작...")
        
        # PDF 처리 파이프라인
        text = processor.pdf_parser(file_path)
        chunks = processor.text_chunking(text)
        embeddings = processor.text_embedding(chunks)
        processor.upload_embeddings_to_es(chunks, embeddings, safe_filename, pdf_name)
        
        print(f"✅ [{pdf_name}] 처리 완료")
        return True
        
    except Exception as e:
        print(f"❌ [{pdf_name}] 처리 실패: {str(e)}")
        return False