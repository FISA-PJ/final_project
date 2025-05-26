import os
from dotenv import load_dotenv
from langchain.schema import Document
from elasticsearch import Elasticsearch
from langchain_elasticsearch import ElasticsearchStore
import logging
from elasticsearch.exceptions import NotFoundError, TransportError
# from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchPythonHook


from plugins.processors.processor1_pdf2md import Parser2Markdown
from plugins.processors.processor2_chunking import HeaderSplitter, SemanticSplitter
from plugins.processors.processor3_embedding import BgeM3Embedding

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 콘솔 핸들러 추가
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 환경 설정
load_dotenv()
os.chdir('/opt/airflow/downloads')

# Elasticsearch 연결
UPSTAGE_API_KEY = os.getenv("UPSTAGE_API_KEY")
preprocessor = Parser2Markdown(UPSTAGE_API_KEY)

embeddings = BgeM3Embedding()
header_splitter = HeaderSplitter()
second_splitter = SemanticSplitter(embeddings)
es =  Elasticsearch('http://airflow-elasticsearch:9200')
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

def process_pdfs():
    # 처리할 PDF 파일 목록 가져오기
    process_files = os.getenv('PROCESS_PDF_FILES', '').split(',')
    if not process_files or process_files[0] == '':
        logger.warning("처리할 PDF 파일이 지정되지 않았습니다.")
        return

    #preprocessor = Parser2Markdown(UPSTAGE_API_KEY)
    #parent_splitter = HeaderSplitter()
    # docstore = ElasticsearchDocstore(
    #     index_name="parent-chunks-00",
    #     es=es,
    # )

    for file_name in process_files:
        file_path = os.path.join('.', file_name)
        if not os.path.exists(file_path):
            logger.error(f"파일을 찾을 수 없습니다: {file_path}")
            continue
        
        logger.info(f"======== 🚩{file_name} 파일에 대한 처리를 시작합니다. ========")

        ## 1. 기존 문서 삭제 (Elasticsearch 쿼리)
        apt_code = file_name.split('_')[0] # file_name
        try:
            if es.indices.exists(index=index_name):
                delete_query = {
                    "query": {
                        "term": {
                            "metadata.apt_code": apt_code
                        }
                    }
                }
                response = es.delete_by_query(index=index_name, body=delete_query)
                deleted_count = response.get("deleted", 0)
                logger.info(f"🗑️ apt_code={apt_code} 관련 문서 {deleted_count}개 삭제 완료")
            else:
                logger.warning(f"⚠️ 인덱스 {index_name}가 존재하지 않아 삭제를 건너뜁니다.")

        except NotFoundError as e:
            logger.warning(f"🚫 삭제 실패: (문서 없음) {e}")

        except TransportError as e:
            logger.error(f"🚨 Elasticsearch 연결 또는 요청 실패: {e.error}, 상태 코드: {e.status_code}")

        except Exception as e:
            logger.error(f"❗알 수 없는 예외 발생: {e}")


        ## 2. PDF문 파싱 및 마크다운 형태로 변환  
        #html_contents = preprocessor.pdf_upstageparser(file_name)
        html_contents = preprocessor.pdf_openparse(file_name)
        # html_contents = read_md_file(file_name)
        markdown_texts = preprocessor.html2md_with_spans(html_contents) 

        doc = Document(
            page_content=markdown_texts,
            metadata={"source_pdf": file_name}
        )
        
        ## 3. 1차 헤더 기반 청크
        header_chunks = header_splitter.split_documents([doc])

        ## 4. 2차 의미 기반 청크
        documents = second_splitter.split_documents(header_chunks)

        ## 5. 일괄 임베딩 + 벡터 저장
        vectorstore.add_documents(documents)
        logger.info(f"-ˋˏ✄┈┈┈┈┈┈┈┈┈┈┈┈ [완료] {len(documents)}개 문서 Elasticsearch에 적재되었습니다. ┈┈┈┈┈┈┈┈┈┈┈┈\n")

if __name__ == "__main__":
    process_pdfs()