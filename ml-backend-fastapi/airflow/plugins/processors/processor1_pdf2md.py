import re
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
import html2text
import openparse
from langchain_upstage import UpstageDocumentParseLoader


class Parser2Markdown:
    def __init__(self, api_key):
        self.api_key = api_key


    def pdf_upstageparser(self, file_path: str) -> str:
        """
        UpstageDocumentParseLoader를 사용해 PDF를 파싱하고,
        HTML 전체를 Markdown 변환 전 단계로 반환합니다.
        
        Args:
            file_path (str): 파싱할 PDF 파일 경로

        Returns:
            str: HTML 태그가 포함된 문서 내용 문자열
        """
        print("======== ⏳ 1. PDF 공고문을 UpstageDocumentParseLoader로 파싱 진행 중입니다. ========")

        loader = UpstageDocumentParseLoader(
            file_path=file_path,
            api_key=self.api_key,
            model="document-parse", 
            split="none",           # Literal['none', 'page', 'element'] = 'none'
            output_format="html",   # Literal['text', 'html', 'markdown'] = 'html'
            ocr="force",            # Literal['auto', 'force'] = 'auto'
        )

        html_contents = loader.load()
        html_content = html_contents[0].page_content

        print("======== INFO: [완료] PDF 공고문 파싱이 완료되었습니다. ========")
        return html_content 
    

    def pdf_openparse(self, file_path: str) -> str : 
        """
        openparse 라이브러리로 PDF를 파싱해 텍스트를 추출합니다

        Args:
            file_path (str): 파싱할 PDF 파일 경로

        Returns:
            str: 추출한 텍스트 문자열
        """
        print("======== ⏳ 1. PDF 공고문을 Openparse로 파싱 진행 중입니다. ========")
        
        parser = openparse.DocumentParser()
        parsed_docs = parser.parse(file_path)
        
        texts = []
        texts = [node.text for node in parsed_docs.nodes if hasattr(node, 'text')]
        text = '\n'.join(texts)
        
        print("======== INFO: [완료] PDF 공고문 파싱이 완료되었습니다. ========")
        return text
    
    
    def flatten_multiindex_columns(self, columns: pd.Index) -> pd.Index:
        """
        MultiIndex 형태의 컬럼명을 '계층1 계층2 ...' 형태로 평탄화합니다.

        Args:
            columns (pd.Index): MultiIndex 컬럼명

        Returns:
            pd.Index: 평탄화된 단일 레벨 컬럼명 리스트
        """
        def _flatten_col(col_tuple):
            seen = []
            for part in col_tuple:
                text = str(part).strip()
                if not seen or text != seen[-1] :
                    seen.append(text)
            return ' '.join([s for s in seen if s])

        return pd.Index([_flatten_col(col) for col in columns])


    def convert_table_to_markdown(self, table_html: str) -> tuple[str, list]:
        """
        HTML <table> 태그를 Markdown 문자열과 JSON 리스트로 변환합니다.

        Args:
            table_html (str): HTML 테이블 문자열

        Returns:
            tuple: (Markdown 테이블 문자열, JSON 형태 리스트)
        """
        df = pd.read_html(StringIO(table_html))[0]

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = self.flatten_multiindex_columns(df.columns)

        #return df.to_markdown(index=False), df.to_dict(orient='records')
        return df.to_dict(orient='records')


    def html2md_with_spans(self, html_content: str) -> tuple[str, list]:
        """
        HTML 문서 내 모든 <table> 태그를 JSON으로 변환 후,
        placeholder로 대체 → JSON 변환을 수행합니다.

        Args:
            html_content (str): HTML 문서 전체 문자열

        Returns:
            최종 Markdown 문자열
        """
        
        print("======== ⏳ 2. HTML 문서를 마크다운 형식으로 변환 중입니다. ========")
        soup = BeautifulSoup(html_content, "html.parser")

        REMOVE_TAGS = ["header", "footer"]
        for tag in soup.find_all(REMOVE_TAGS):  # 태그 제거
            tag.decompose()
        for br in soup.find_all("br"):
            br.replace_with(" ")

        table_placeholders = []
        for idx, table in enumerate(soup.find_all("table")):
            json_table = self.convert_table_to_markdown(str(table))
            table_placeholder = f"__TABLE_PLACEHOLDER_{idx}__"
            table_placeholders.append((table_placeholder, json_table))
            table.replace_with(table_placeholder)


        h = html2text.HTML2Text()
        h.body_width = 0    # 자동 줄바꿈 False 
        markdown_text = h.handle(str(soup))
        markdown_text = re.sub(r'\n+', '\n', markdown_text).strip()

        # JSON 형태로 변환
        for table_placeholder, json_table in table_placeholders:
            markdown_text = markdown_text.replace(table_placeholder, f"{json_table}")

        print("======== INFO: [완료] 마크다운 형식으로 변환되었습니다. ========\n")
        return markdown_text