import re
import hashlib
import logging
from typing import List, Any
from langchain.schema import Document
from langchain.text_splitter import TextSplitter
from langchain_experimental.text_splitter import SemanticChunker

# 로깅 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers = []  # 기존 핸들러 제거
handler = logging.StreamHandler()  # stdout으로 출력
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class HeaderSplitter() :
    def __init__(self, min_chunk_length : int = 50):
        self.min_chunk_length = min_chunk_length
        
        
    def split_by_headers(self, text : str) -> list[str] :
        """
        '#' 헤더 기준으로 텍스트 분할
        Args:
            text (str): 분할할 텍스트
        Returns:
            List[str]: 헤더 기준으로 나뉜 텍스트 리스트
        """
        # 헤더로 시작하는 위치 찾기
        header_positions = [m.start() for m in re.finditer("#", text, re.MULTILINE)]

        if not header_positions: 
            logger.info("▶ INFO: 헤더가 없습니다. 따라서 전체 텍스트를 반환하여 ")
            return [text.strip()]
        
        header_positions.append(len(text))
        raw_chunks = [
            text[header_positions[i]:header_positions[i+1]].strip()
            for i in range(len(header_positions)-1)
        ]

        # 짧은 청크 병합 처리
        chunks = []
        for idx, chunk in enumerate(raw_chunks):
            chunk = chunk.strip()
            if len(chunk) < self.min_chunk_length:
                if idx == len(raw_chunks) - 1 and chunks:
                    chunks[-1] += '\n' + chunk
                else:
                    raw_chunks[idx + 1] = chunk + '\n' + raw_chunks[idx + 1]
            else:
                chunks.append(chunk)

        return chunks


    def extract_header_title(self, chunk: str) -> str:
        """
        청크 내부 첫 번째 헤더 텍스트 추출
        Args:
            chunk (str): 텍스트 청크
        Returns:
            str: 헤더 텍스트 (예: '### 시설 현황')
        """
        match = re.search(r"(?m)^(#+\s*)(.+)", chunk)
        return match.group(2).strip() if match else "unknown"
    
    
    def split_documents(self, docs: List[Document]) -> List[Document]:
        """
        문서 리스트를 헤더 기준으로 분할
        Args:
            docs (List[Document]): 분할할 문서 리스트
        Returns:
            List[Document]: 분할된 문서 객체 리스트
        """
        
        logger.info("======== ⏳ 3. Header 기준으로 1차 Chunking 진행 중입니다. ========")
        output = []

        for doc in docs :
            pdf_name = doc.metadata.get("source_pdf", "unknown.pdf")
            chunks = self.split_by_headers(doc.page_content)

            for chunk in chunks:
                metadata = {
                    "source_pdf": pdf_name,                 
                    "apt_code": pdf_name.split("_")[0] ,    # 공고 코드 
                    "header" : self.extract_header_title(chunk)
                }
                output.append(Document(page_content=chunk, metadata=metadata))#  List[Document]
                
        logger.info("======== INFO: [완료] Header 구조에 따라 문서를 분할했습니다.  ========")
        logger.info(f"▶ADDITIONAL INFO: {len(output)} 개의 Header Chunk가 생성되었습니다.\n")
        return output
    
    
class SemanticSplitter():
    def __init__(self, embed_model: Any, breakpoint_threshold: int = 70, min_child_length: int = 500):
        """
        Args:
            embed_model (object): 임베딩 모델 객체
            breakpoint_threshold (int): 분할 임계값 (percentile)
            min_child_length (int): 자식 문서 청킹 기준
        """
        self.embed_model = embed_model
        self.breakpoint_threshold = breakpoint_threshold
        self.min_child_length = min_child_length
    
    
    def _apply_overlap(self, chunks: List[str], overlap_size: int = 200) -> List[str] :
        """청크 간 오버랩 처리"""
        overlapped = []
        for i, chunk in enumerate(chunks) :
            prefix = chunks[i - 1][-overlap_size:] if i > 0 and len(chunks[i - 1]) > overlap_size else ""
            suffix = chunks[i + 1][:overlap_size] if i < len(chunks) - 1 and len(chunks[i + 1]) > overlap_size else ""
            overlapped.append(prefix + chunk + suffix)
            
        return overlapped
            
            
    def split_by_semantic(self, text: str) -> List[str]:
        """
        의미 기반 텍스트 분할 후 오버랩 적용
        Args:
            text (str): 입력 텍스트
        Returns:
            List[str]: 분할된 텍스트 리스트 (with overlap)
        """
        chunker = SemanticChunker(
            self.embed_model,
            breakpoint_threshold_type="percentile",
            breakpoint_threshold_amount=self.breakpoint_threshold,
            min_chunk_size=self.min_child_length
        )

        chunks = [doc.page_content for doc in chunker.create_documents([text])]
        return self._apply_overlap(chunks)
    
    
    def split_documents(self, docs: List[Document]) -> List[Document]:
        """
        Args:
            docs (List[Document]): 분할할 문서 리스트
        Returns:
            List[Document]: 모든 분할된 Document 리스트
        """
        logger.info("======== ⏳ 4. Semantic Chunking 전체 문서 처리 중입니다. ========")
        output: List[Document] = []

        for i, doc in enumerate(docs):
            text_blocks = (
                [doc.page_content] if len(doc.page_content) < self.min_child_length
                else self.split_by_semantic(doc.page_content)
            )

            for j ,block in enumerate(text_blocks):
                metadata = dict(doc.metadata)
                metadata["chunk_id"] = f"{i}-{j}"

                output.append(Document(page_content=block, metadata=metadata))

        logger.info("======== INFO: [완료] 문서에 대해 semantic 기반으로 청크를 분할했습니다. ========")
        logger.info(f"▶ ADDITIONAL INFO: 총 {len(output)} 개의 Semantic 청크가 생성되었습니다.\n")
        return output