import torch
from torch import Tensor
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel
from langchain.embeddings.base import Embeddings
from typing import List
from tqdm import tqdm
import sys

import logging

# 로깅 설정
logger = logging.getLogger(__name__)
logging.getLogger('tqdm').setLevel(logging.INFO)# 로깅 설정
logger.setLevel(logging.INFO)
logger.handlers = []  # 기존 핸들러 제거
handler = logging.StreamHandler()  # stdout으로 출력
formatter = logging.Formatter('%(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# tqdm 설정
tqdm.monitor_interval = 0
tqdm.write = lambda s: sys.stdout.write(f"{s}\n")

class BgeM3Embedding(Embeddings):
    def __init__(self, batch_size: int = 16, model_name: str = "BAAI/bge-m3", max_token_length: int = 8192, chunk_token_size: int = 5000):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        self.batch_size = batch_size
        self.max_token_length = max_token_length
        self.chunk_token_size = chunk_token_size
        self.model.eval()


    def average_pool(self, last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(last_hidden_states.size()).float()
        sum_embeddings = torch.sum(last_hidden_states * input_mask_expanded, 1)
        sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
        return sum_embeddings / sum_mask


    def _split_tokens(self, tokens: List[int]) -> List[List[int]]:
        return [tokens[i:i + self.chunk_token_size] for i in range(0, len(tokens), self.chunk_token_size)]


    def _embed_text(self, text: str, prefix: str = "passage") -> torch.Tensor:
        formatted = f"{prefix}: {text}"
        inputs = self.tokenizer(
            formatted,
            padding=True,
            truncation=True,
            return_tensors="pt",
            max_length=self.max_token_length
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model(**inputs)
            pooled = self.average_pool(outputs.last_hidden_state, inputs["attention_mask"])
            return F.normalize(pooled, p=2, dim=1)[0]


    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        
        all_embeddings = []

        for text in tqdm(texts, desc="Embedding documents", leave=True):
            tokens = self.tokenizer.encode(text, add_special_tokens=False)

            if len(tokens) <= self.max_token_length:
                embeddings = self._embed_text(text)
            else : 
                tqdm.write(f"▶ADDITIONAL INFO: {len(tokens)}개의 토큰은 입력 토큰을 초과하는 관계로, 텍스트를 나눠 처리합니다.")
                token_chunks = self._split_tokens(tokens)
                chunk_texts = [self.tokenizer.decode(chunk) for chunk in token_chunks]
                chunk_embeddings  = [self._embed_text(text, prefix = "passage") for text in chunk_texts]
                embeddings  = torch.mean(torch.stack(chunk_embeddings ), dim=0)
                
            all_embeddings.append(embeddings .cpu().tolist())

        logger.info("======== INFO: [완료] 임베딩을 성공적으로 수행하였습니다. ========")
        return all_embeddings


    def embed_query(self, text: str) -> List[float]:
        return self._embed_text(text, prefix="query").cpu().tolist()