import os
import re
import hashlib
from typing import List
from langchain.schema import Document
from langchain.text_splitter import TextSplitter
from langchain_experimental.text_splitter import SemanticChunker


class HeaderSplitter() :
    def __init__(self, min_chunk_length : int = 50):
        self.min_chunk_length = min_chunk_length
        
        
    def split_by_headers(self, text : str) -> list[str] :
        """
        텍스트를 '#' 헤더 기준으로 분할합니다.

        Args:
            text (str): 분할할 텍스트

        Returns:
            List[str]: 헤더 기준으로 나뉜 텍스트 리스트
        """
        
        # 헤더로 시작하는 위치 찾기
        header_positions = [m.start() for m in re.finditer("#", text, re.MULTILINE)]

        if not header_positions: 
            print("▶ INFO: 헤더가 없습니다. 따라서 전체 텍스트를 반환하여 ")
            return [text]
        
        header_positions.append(len(text))

        raw_chunks = [
            text[header_positions[i]:header_positions[i+1]]
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
        TextSplitter 추상 메서드 override
        문서 리스트를 헤더 기준으로 분할 후, doc_id 생성 및 메타데이터 추가

        Args:
            docs (List[Document]): 분할할 문서 리스트

        Returns:
            List[Document]: 분할된 문서 객체 리스트
        """
        
        print("======== ⏳ 3. Header 기준으로 1차 Chunking 진행 중입니다. ========")
        output: List[Document] = []

        for doc in docs :
            pdf_name = doc.metadata.get("source_pdf", "unknown.pdf")

            chunks = self.split_by_headers(doc.page_content)

            for i, chunk in enumerate(chunks):
                metadata = {
                    "source_pdf": pdf_name,                 
                    "apt_code": pdf_name.split("_")[0] ,    # 주택 코드 
                    "header" : self.extract_header_title(chunk)
                }
                output.append(Document(page_content=chunk, metadata=metadata))#  List[Document]
                #print(f'{i} 번째 Header 문서의 Metadata :\n{metadata}')
                
        print("======== INFO: [완료] Header 구조에 따라 문서를 분할했습니다.  ========")
        print(f"▶ADDITIONAL INFO: {len(output)} 개의 Header Chunk가 생성되었습니다.\n")
        return output
    
    
class SemanticSplitter():
    def __init__(self, embed_model: object, breakpoint_threshold: int = 70, min_child_length: int = 500):
        """
        Args:
            embed_model (object): 임베딩 모델 객체
            breakpoint_threshold (int): 분할 임계값 (percentile)
            min_child_length (int): 자식 문서 청킹 기준
        """
        #super().__init__()
        self.embed_model = embed_model
        self.breakpoint_threshold = breakpoint_threshold
        self.min_child_length = min_child_length
    
    def split_by_semantic(self, text: str) -> List[str]:
        """
        의미 기반 청킹 수행 + 청크 간 오버랩 적용

        Args:
            text (str): 입력 텍스트

        Returns:
            List[str]: 분할된 텍스트 리스트 (with overlap)
        """
        semantic_chunker = SemanticChunker(
            self.embed_model,
            breakpoint_threshold_type="percentile",
            breakpoint_threshold_amount=self.breakpoint_threshold,
            min_chunk_size=self.min_child_length
        )

        semantic_chunks = semantic_chunker.create_documents([text])
        split_chunks = [doc.page_content for doc in semantic_chunks]

        # ✅ 청크 간 앞뒤 200자 오버랩 적용
        overlapped_chunks = []

        for i, chunk in enumerate(split_chunks):
            prefix = ""
            suffix = ""

            if i > 0:
                prev = split_chunks[i - 1]
                prefix = prev[-200:] if len(prev) > 200 else prev

            if i < len(split_chunks) - 1:
                next_chunk = split_chunks[i + 1]
                suffix = next_chunk[:200] if len(next_chunk) > 200 else next_chunk

            combined = prefix + chunk + suffix
            overlapped_chunks.append(combined)

        return overlapped_chunks

    
    def split_documents(self, docs: List[Document]) -> List[Document]:
        """
        의미 기반으로 여러 문서 분할

        Args:
            docs (List[Document]): 분할할 문서 리스트

        Returns:
            List[Document]: 모든 분할된 Document 리스트
        """
        print("======== ⏳ 4. Semantic Chunking 전체 문서 처리 중입니다. ========")
        output: List[Document] = []

        for doc in docs:
            apt_code = doc.metadata.get("apt_code", "unknown")

            if len(doc.page_content) < self.min_child_length:
                child_texts = [doc.page_content]
            else:
                child_texts = self.split_by_semantic(doc.page_content)

            for child_text in child_texts:
                #print(f"child_text_length : {len(child_text)}")
                base = child_text + str(apt_code)
                child_id = hashlib.sha256(base.encode("utf-8")).hexdigest()

                metadata = dict(doc.metadata)
                metadata["child_id"] = child_id

                output.append(Document(page_content=child_text, metadata=metadata))

        print("======== INFO: [완료] 문서에 대해 semantic 기반으로 청크를 분할했습니다. ========")
        print(f"▶ADDITIONAL INFO: 총 {len(output)} 개의 Semantic 청크가 생성되었습니다.\n")
        return output