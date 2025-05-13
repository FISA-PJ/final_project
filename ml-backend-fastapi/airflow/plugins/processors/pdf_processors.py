import torch
import torch.nn.functional as F
from torch import Tensor
from typing import List, Optional
from datetime import datetime
from transformers import AutoTokenizer, AutoModel
from langchain.text_splitter import RecursiveCharacterTextSplitter
from elasticsearch import Elasticsearch, helpers
import openparse

# PDF 처리 클래스 최적화
class PDFProcessor:
    """
    PDF 문서 처리 및 임베딩 생성 클래스
    
    이 클래스는 PDF 문서를 파싱하여 텍스트를 추출하고, 추출된 텍스트를 청크 단위로 분할한 후,
    각 청크에 대한 임베딩 벡터를 생성하여 Elasticsearch에 저장하는 기능을 제공합니다.
    모든 처리 과정은 배치 처리 방식으로 최적화되어 있습니다.
    
    Attributes:
    -----------
    es : Elasticsearch
        Elasticsearch 클라이언트 인스턴스
    index_name : str
        임베딩이 저장될 Elasticsearch 인덱스 이름
    batch_size : int
        배치 처리 시 청크 단위의 크기
    model_name : str
        임베딩 생성에 사용할 사전 학습된 모델의 이름
    _tokenizer : AutoTokenizer
        텍스트 토큰화를 위한 토크나이저 인스턴스 (지연 초기화)
    _model : AutoModel
        임베딩 생성을 위한 모델 인스턴스 (지연 초기화)
    """
    
    def __init__(
        self, 
        es: Elasticsearch, 
        batch_size: int, 
        index_name: str, 
        model_name: str = 'intfloat/multilingual-e5-small'
    ):
        """
        PDFProcessor 클래스 초기화
        
        Parameters:
        -----------
        es : Elasticsearch
            Elasticsearch 클라이언트 인스턴스
        batch_size : int
            배치 처리 시 청크 단위의 크기
        index_name : str
            임베딩이 저장될 Elasticsearch 인덱스 이름
        model_name : str, optional
            임베딩 생성에 사용할 사전 학습된 모델의 이름, 기본값은 'intfloat/multilingual-e5-small'
        """
        self.es = es
        self.index_name = index_name
        self.batch_size = batch_size
        # 토크나이저 및 모델 로드는 지연 초기화 (필요할 때만 로드)
        self._tokenizer = None
        self._model = None
        self.model_name = model_name
        
        # 📝수정 - device 설정
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"▶️ Using device: {self.device}")
        
        # 인덱스 존재 확인 및 생성
        self._ensure_index_exists()
    
    @property
    def tokenizer(self):
        """
        토크나이저 지연 초기화 프로퍼티
        
        토크나이저를 처음 사용할 때만 로드하여 메모리 사용을 최적화합니다.
        
        Returns:
        --------
        AutoTokenizer
            초기화된 토크나이저 인스턴스
        """
        if self._tokenizer is None:
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        return self._tokenizer
    
    @property
    def model(self):
        """
        모델 지연 초기화 프로퍼티
        
        모델을 처음 사용할 때만 로드하여 메모리 사용을 최적화합니다.
        
        Returns:
        --------
        AutoModel
            초기화된 임베딩 모델 인스턴스
        """
        if self._model is None:
            # 수정: to_empty() 사용 후 load_state_dict()로 모델 로드
            self._model = AutoModel.from_pretrained(self.model_name)
            if torch.cuda.is_available():
                # 먼저 CPU에 모델 로드 후 GPU로 이동
                self._model = self._model.to(self.device)
        return self._model
    
    def _ensure_index_exists(self):
        """
        필요한 Elasticsearch 인덱스가 존재하는지 확인하고 없으면 생성
        
        이 메서드는 클래스 초기화 시 자동으로 호출되어 필요한 인덱스가 
        Elasticsearch에 존재하는지 확인하고, 없을 경우 적절한 매핑으로 생성합니다.
        특히 임베딩을 위한 dense_vector 필드를 적절히 구성합니다.
        """
        if not self.es.indices.exists(index=self.index_name):
            # 인덱스 생성 (임베딩 필드 매핑 포함)
            mapping = {
                "mappings": {
                    "properties": {
                        "content": {"type": "text"},
                        "embedding": {
                            "type": "dense_vector",
                            "dims": 384,  # multilingual-e5-small 모델의 임베딩 차원
                            "index": True,
                            "similarity": "cosine"
                        },
                        "source_pdf": {"type": "keyword"},
                        "chunk_id": {"type": "integer"}
                    }
                }
            }
            self.es.indices.create(index=self.index_name, body=mapping)
            print(f"✅ 인덱스 '{self.index_name}' 생성 완료")
    
    def pdf_parser(self, file_path: str) -> str:
        """
        PDF 문서 파싱하여 텍스트 추출
        
        openparse 라이브러리를 사용하여 PDF 파일에서 텍스트를 추출합니다.
        
        Parameters:
        -----------
        file_path : str
            파싱할 PDF 파일의 경로
            
        Returns:
        --------
        str
            PDF에서 추출된 전체 텍스트
            
        Raises:
        -------
        Exception
            PDF 파싱 중 발생하는 모든 예외
        """
        try:
            parser = openparse.DocumentParser()
            parsed_docs = parser.parse(file_path)
            # 리스트 컴프리헨션으로 최적화
            text_list = [node.text for node in parsed_docs.nodes if hasattr(node, 'text')]
            return '\n'.join(text_list)
        except Exception as e:
            print(f"⚠️ PDF 파싱 오류: {str(e)}")
            raise
    
    def text_chunking(self, text: str, chunk_size: int = 2000, chunk_overlap: int = 200) -> List[str]:
        """
        텍스트를 적절한 크기로 분할
        
        긴 텍스트를 처리하기 쉬운 크기의 청크로 분할합니다.
        RecursiveCharacterTextSplitter를 사용하여 문맥을 유지하면서 텍스트를 분할합니다.
        
        Parameters:
        -----------
        text : str
            분할할 원본 텍스트
        chunk_size : int, optional
            각 청크의 최대 문자 수, 기본값은 2000
        chunk_overlap : int, optional
            연속된 청크 간의 중복 문자 수, 기본값은 200
            
        Returns:
        --------
        List[str]
            분할된 텍스트 청크의 리스트
        """
        if not text.strip():
            return []
            
        splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        chunks = splitter.split_text(text)
        print(f"☺️ 청킹 결과: {len(chunks)}개 청크 생성되었습니다.")
        return chunks
    
    def average_pool(self, last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
        """
        텍스트 임베딩 생성을 위한 평균 풀링
        
        토큰 임베딩을 평균 풀링하여 텍스트 임베딩 벡터를 생성합니다.
        마스킹된 토큰(패딩)은 평균 계산에서 제외됩니다.
        
        Parameters:
        -----------
        last_hidden_states : Tensor
            모델의 마지막 은닉층 출력
        attention_mask : Tensor
            패딩 토큰을 마스킹하기 위한 어텐션 마스크
            
        Returns:
        --------
        Tensor
            평균 풀링된 임베딩 벡터
        """
        last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
        return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]
    
    def text_embedding(self, chunks: List[str]) -> List[List[float]]:
        """
        텍스트 청크를 임베딩 벡터로 변환
        
        텍스트 청크 리스트를 받아 각 청크에 대한 임베딩 벡터를 생성합니다.
        배치 처리 방식으로 효율적으로 임베딩을 생성합니다.
        
        Parameters:
        -----------
        chunks : List[str]
            임베딩할 텍스트 청크 리스트
            
        Returns:
        --------
        List[List[float]]
            생성된 임베딩 벡터의 리스트 (각 벡터는 float 리스트)
        """
        if not chunks:
            return []
            
        # 배치 처리 최적화
        all_embeddings = []
        for i in range(0, len(chunks), self.batch_size):
            batch = chunks[i:i+self.batch_size]
            
            # 메모리 효율성 개선을 위해 with torch.no_grad() 추가
            with torch.no_grad():
                batch_dict = self.tokenizer(
                    batch,
                    max_length=512,
                    padding=True,
                    truncation=True,
                    return_tensors='pt'
                )
                
                # 모델 추론 - 텐서를 device로 이동 (안전하게 처리)
                if torch.cuda.is_available():
                    input_ids = batch_dict['input_ids'].to(self.device)
                    attention_mask = batch_dict['attention_mask'].to(self.device)
                    outputs = self.model(input_ids=input_ids, attention_mask=attention_mask)
                else:
                    outputs = self.model(**batch_dict)

                # 평균 풀링과 정규화
                if torch.cuda.is_available():
                    embeddings = self.average_pool(outputs.last_hidden_state, attention_mask)
                else:
                    embeddings = self.average_pool(outputs.last_hidden_state, batch_dict['attention_mask'])

                embeddings = F.normalize(embeddings, p=2, dim=1)

                # CPU로 이동하여 저장
                all_embeddings.append(embeddings.cpu())
                 
        # 결과 결합
        result = torch.cat(all_embeddings, dim=0).tolist() if all_embeddings else []
        print(f"☺️ 임베딩 결과: {len(result)}개의 임베딩이 모두 완료되었습니다")
        return result
    
    def upload_embeddings_to_es(
        self, 
        chunks: List[str], 
        embeddings: List[List[float]], 
        safe_filename: str, 
        pdf_name: str
    ):
        """
        임베딩 벡터를 Elasticsearch에 업로드
        
        생성된 임베딩 벡터와 관련 메타데이터를 Elasticsearch에 벌크 업로드합니다.
        
        Parameters:
        -----------
        chunks : List[str]
            텍스트 청크 리스트
        embeddings : List[List[float]]
            각 청크에 해당하는 임베딩 벡터 리스트
        safe_filename : str
            저장된 PDF 파일의 안전한 파일명 (파일 식별자로 사용)
        pdf_name : str
            원본 PDF 파일명
            
        Notes:
        ------
        벌크 업로드 방식을 사용하여 Elasticsearch에 데이터를 효율적으로 저장합니다.
        각 문서에는 청크 내용, 임베딩 벡터, 소스 파일 정보, 청크 ID 등이 포함됩니다.
        """
        if not chunks or not embeddings:
            print("⚠️ 업로드할 청크나 임베딩이 없습니다.")
            return
            
        # 벌크 업로드 최적화
        for i in range(0, len(chunks), self.batch_size):
            actions = []
            chunk_batch = chunks[i:i+self.batch_size]
            embedding_batch = embeddings[i:i+self.batch_size]
            
            # 벌크 액션 구성
            for idx, (chunk, embedding) in enumerate(zip(chunk_batch, embedding_batch)):
                doc_id = f"{pdf_name}_{i+idx}"
                action = {
                    '_index': self.index_name,
                    '_id': doc_id,
                    '_source': {
                        'content': chunk,
                        'embedding': embedding,
                        'source_pdf': safe_filename,
                        'chunk_id': i+idx,
                        'timestamp': datetime.now().isoformat()
                    }
                }
                actions.append(action)
            
            # 벌크 업로드 실행
            if actions:
                helpers.bulk(self.es, actions, raise_on_error=True)
        
        print(f"☺️ {pdf_name}의 {len(chunks)}개 청크를 ES에 성공적으로 저장했습니다")