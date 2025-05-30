import re
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from typing import List, Tuple
import html2text
import openparse
from langchain_upstage import UpstageDocumentParseLoader

import logging

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # 기존 핸들러 제거
handler = logging.StreamHandler()  # stdout으로 출력
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class Parser2Markdown:
    def __init__(self, api_key):
        self.api_key = api_key


    def pdf_upstageparser(self, file_path: str) -> str:
        """
        UpstageDocumentParseLoader로 PDF문 파싱 후, HTML 태그르르 마크다운으로 변환 
        Args:
            file_path (str): 파싱할 PDF 파일 경로

        Returns:
            str: HTML 태그가 포함된 문서 내용 문자열
        """
        logger.info("======== ⏳ 1. PDF 공고문을 UpstageDocumentParseLoader로 파싱 진행 중입니다. ========")

        loader = UpstageDocumentParseLoader(
            file_path=file_path,
            api_key=self.api_key,
            model="document-parse", 
            split="none",           # Literal['none', 'page', 'element'] = 'none'
            output_format="html",   # Literal['text', 'html', 'markdown'] = 'html'
            ocr="force",            # Literal['auto', 'force'] = 'auto'
        )
        html_contents = loader.load()

        logger.info("======== INFO: [완료] PDF 공고문 파싱이 완료되었습니다. ========")
        return html_contents[0].page_content
    

    def pdf_openparse(self, file_path: str) -> str : 
        """
        openparse 라이브러리로 PDF를 파싱해 텍스트를 추출합니다

        Args:
            file_path (str): 파싱할 PDF 파일 경로

        Returns:
            str: 추출한 텍스트 문자열
        """
        logger.info("======== ⏳ 1. PDF 공고문을 Openparse로 파싱 진행 중입니다. ========")
        
        parser = openparse.DocumentParser()
        parsed_docs = parser.parse(file_path)
        
        texts = [node.text for node in parsed_docs.nodes if hasattr(node, 'text')]
        
        logger.info("======== INFO: [완료] PDF 공고문 파싱이 완료되었습니다. ========")
        return '\n'.join(texts)
    
    
    def flatten_multiindex_columns(self, columns: pd.Index) -> pd.Index:
        """
        MultiIndex 형태의 컬럼명을 '계층1 계층2 ...' 형태로 flatten
        Args:
            columns (pd.Index): MultiIndex 컬럼명
        Returns:
            pd.Index: 평탄화된 단일 레벨 컬럼명 리스트
        """
        def _flatten_col(col_tuple):
            """
            Args:
                col_tuple (tuple): MultiIndex의 단일 컬럼 (예: ('항목', '항목', '금액'))
            Returns:
                str: 평탄화된 컬럼명 (예: '항목 금액')
            """
            flattened_parts = []
            previous = None

            for part in col_tuple:
                current = str(part).strip()
                if current and current != previous: # 중복된 내용이 없는 경우 
                    flattened_parts.append(current)
                    previous = current
            return " ".join(flattened_parts) # 공백으로 구분 

        return pd.Index([_flatten_col(col) for col in columns])


    def convert_table_to_json(self, table_html: str) -> list:
        """
        HTML <table> 태그를 dictionary로 반환
        Args:
            table_html (str): HTML 테이블 문자열
        Returns:
            List[Dict]: 테이블 행(row) 단위의 JSON 리스트
        """
        df = pd.read_html(StringIO(table_html))[0]

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = self.flatten_multiindex_columns(df.columns)

        return df.fillna('').to_dict(orient='records')


    def html_to_markdown_with_tables(self, html_content: str) -> str:
        """
        HTML 문서에서 <table>을 JSON으로 변환해 placeholder로 대체한 후, 
        전체 문서를 Markdown 형식으로 변환
        Args:
            html_content (str): HTML 문서 전체 문자열
        Returns:
            최종 Markdown 문자열
        """
        
        logger.info("======== ⏳ 2. HTML 문서를 마크다운 형식으로 변환 중입니다. ========")
        soup = BeautifulSoup(html_content, "html.parser")

        REMOVE_TAGS = ["header", "footer"]
        for tag in soup.find_all(REMOVE_TAGS):  # 태그 제거
            tag.decompose()
            
        for br in soup.find_all("br"):
            br.replace_with(" ")

        table_placeholders = []
        for idx, table in enumerate(soup.find_all("table")):
            json_list = self.convert_table_to_json(str(table))
            table_placeholder = f"__TABLE_PLACEHOLDER_{idx}__"
            table_placeholders.append((table_placeholder, json_list))
            table.replace_with(table_placeholder)


        h = html2text.HTML2Text()
        h.body_width = 0    # 자동 줄바꿈 False 
        markdown_text = h.handle(str(soup))
        markdown_text = re.sub(r'\n+', '\n', markdown_text).strip()

        # Placeholder 대체
        for table_placeholder, json_list in table_placeholders:
            markdown_text = markdown_text.replace(table_placeholder, f"{json_list}")

        logger.info("======== INFO: [완료] 마크다운 형식으로 변환되었습니다. ========\n")
        return markdown_text