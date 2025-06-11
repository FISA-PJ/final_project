import os
from dotenv import load_dotenv
from langchain.schema import Document
from elasticsearch import Elasticsearch
from langchain_elasticsearch import ElasticsearchStore
import logging
from elasticsearch.exceptions import NotFoundError, TransportError

from plugins.processors.loader import Parser2Markdown
from plugins.processors.chunker import HeaderSplitter, SemanticSplitter
from plugins.processors.embedding import BgeM3Embedding

from datetime import datetime
import json

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # 기존 핸들러 제거
handler = logging.StreamHandler()  # stdout으로 출력
formatter = logging.Formatter("%(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# 테스트 로그
# logger.info("main.py 실행 시작")

# 환경 설정
load_dotenv()

# 수정된 코드 - 디렉토리 존재 여부 확인
target_dir = "/opt/airflow/downloads/normal_dag"
if os.path.exists(target_dir):
    os.chdir(target_dir)
else:
    # 디렉토리 생성 또는 다른 처리
    os.makedirs(target_dir, exist_ok=True)
    os.chdir(target_dir)

# Elasticsearch 연결
UPSTAGE_API_KEY = ''
preprocessor = Parser2Markdown(UPSTAGE_API_KEY)

embeddings = BgeM3Embedding()
header_splitter = HeaderSplitter()
second_splitter = SemanticSplitter(embeddings)
es = Elasticsearch("http://airflow-elasticsearch:9200")
index_name = "test-0524-tmp"

vectorstore = ElasticsearchStore(
    index_name=index_name,
    embedding=embeddings,
    es_connection=es,
)

# # 이미 파싱된 파일 읽어오는 경우
# def read_md_file(file_path):
#     with open(file_path, 'r', encoding='utf-8') as f:
#         return f.read()


def delete_documents_by_apt_code(es: Elasticsearch, index: str, apt_code: str):
    if not es.indices.exists(index=index):
        logger.info(f"⚠️ 인덱스 '{index}'가 존재하지 않아 삭제를 건너뜁니다.")
        return

    query = {"query": {"term": {"metadata.apt_code": apt_code}}}

    try:
        response = es.delete_by_query(index=index, body=query)
        deleted = response.get(deleted, 0)
        logger.info(
            f"🗑️ apt_code={apt_code} 문서 {deleted}건 삭제 완료"
            if deleted
            else f"ℹ️ apt_code={apt_code} 관련 문서 없음"
        )

    except NotFoundError as e:
        logger.info(f"🚫 문서 없음: {e}")
    except TransportError as e:
        logger.info(f"🚨 요청 실패: {e.error}, 상태 코드: {e.status_code}")
    except Exception as e:
        logger.info(f"❗ 예외 발생: {e}")


def process_pdfs():
    # 실행 날짜 가져오기
    execution_date = os.getenv(
        "AIRFLOW_EXECUTION_DATE", datetime.now().strftime("%Y-%m-%d")
    )

    # ES 업로드 실패 기록 디렉터리 생성
    failed_records_dir = "esupload_failed_records"
    os.makedirs(failed_records_dir, exist_ok=True)

    # 날짜별 실패 기록 파일 경로
    failed_file_path = os.path.join(
        failed_records_dir, f"{execution_date}_failed_files.json"
    )

    # 처리할 PDF 파일 목록 가져오기
    process_files_json = os.getenv("PROCESS_PDF_FILES", "[]")
    process_files = json.loads(process_files_json)

    # ES 적재 실패한 파일 목록 담을 리스트 생성
    failed_files = []

    # 파일이 지정되지 않은 경우 경고 로그 출력
    if not process_files or process_files[0] == "":
        logger.warning("처리할 PDF 파일이 지정되지 않았습니다.")
        return

    for file_name in process_files:
        file_path = os.path.join("/opt/airflow/downloads/normal_dag", file_name)
        try:
            if not os.path.exists(file_path):
                logger.error(f"파일을 찾을 수 없습니다: {file_path}")
                continue

            logger.info(
                f"======== 🚩{file_name} 파일에 대한 처리를 시작합니다. ========"
            )

            ## 1. 기존 문서 삭제 (Elasticsearch 쿼리)
            apt_code = file_name.split("_")[0]  # file_name
            delete_documents_by_apt_code(es, index_name, apt_code)

            ## 2. PDF문 파싱 및 마크다운 형태로 변환
            html_contents = preprocessor.pdf_upstageparser(file_name)
            # html_contents = preprocessor.pdf_openparse(file_name)
            # html_contents = read_md_file(file_name)
            markdown_texts = preprocessor.html_to_markdown_with_tables(html_contents)

            doc = Document(
                page_content=markdown_texts, metadata={"source_pdf": file_name}
            )

            ## 3. 1차 헤더 기반 청크
            header_chunks = header_splitter.split_documents([doc])

            ## 4. 2차 의미 기반 청크
            final_documents = second_splitter.split_documents(header_chunks)

            ## 5. 일괄 임베딩 + 벡터 저장
            vectorstore.add_documents(final_documents)
            logger.info(
                f"-ˋˏ✄┈┈┈┈┈┈┈┈┈┈┈┈ [완료] {len(final_documents)}개 문서 Elasticsearch에 적재되었습니다. ┈┈┈┈┈┈┈┈┈┈┈┈\n"
            )

        except Exception as e:
            logger.error(f"{file_name} ES 적재 실패: {str(e)}")
            logger.error(f"파일 {file_name} 을 적재 실패 목록 리스트에 추가합니다.")
            failed_files.append(file_name)
            continue  # 다음 파일로 계속 진행

    # 실패한 파일 목록 저장
    if failed_files:
        with open(failed_file_path, "w") as f:
            json.dump(
                {
                    "date": execution_date,  # 실행 날짜
                    "failed_files": failed_files,  # 실패한 PDF 파일명 목록
                },
                f,
                indent=2,
            )
        logger.info(f"ES 적재 실패 파일 목록을 {failed_file_path}에 저장했습니다.")
    # logger.info("main.py 실행 완료")


if __name__ == "__main__":
    process_pdfs()
