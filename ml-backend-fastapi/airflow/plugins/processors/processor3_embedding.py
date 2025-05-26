import torch
from torch import Tensor
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel
from langchain.embeddings.base import Embeddings
from typing import List
from tqdm import tqdm


class BgeM3Embedding(Embeddings):
    def __init__(self, bath_size: int = 16, model_name: str = "BAAI/bge-m3"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        self.batch_size = bath_size
        self.model.eval()


    def average_pool(self, last_hidden_states: Tensor, attention_mask: Tensor) -> Tensor:
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(last_hidden_states.size()).float()
        sum_embeddings = torch.sum(last_hidden_states * input_mask_expanded, 1)
        sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
        return sum_embeddings / sum_mask
    

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        
        all_embeddings = []

        for text in tqdm(texts, desc="Embedding documents", leave=True):
            tokens = self.tokenizer.encode(text, add_special_tokens=False)

            if len(tokens) > 8192:
                tqdm.write(f"▶ADDITIONAL INFO: {len(tokens)}개의 토큰은 입력 토큰을 초과하는 관계로, 텍스트를 나눠 처리합니다.")
                chunks = self._split_tokens(tokens, chunk_size=5000)
                chunk_texts = [self.tokenizer.decode(chunk) for chunk in chunks]
                embeddings = [self._embed_text(t, prefix = "passage") for t in chunk_texts]
                mean_embedding = torch.mean(torch.stack(embeddings), dim=0)
            else:
                mean_embedding = self._embed_text(text)

            all_embeddings.append(mean_embedding.cpu().tolist())

        print("======== INFO: [완료] 임베딩을 성공적으로 수행하였습니다. ========")
        return all_embeddings


    def _split_tokens(self, tokens: List[int], chunk_size: int) -> List[List[int]]:
        return [tokens[i:i + chunk_size] for i in range(0, len(tokens), chunk_size)]


    def _embed_text(self, text: str, prefix: str = "passage") -> torch.Tensor:
        formatted = f"{prefix}: {text}"
        inputs = self.tokenizer(
            formatted,
            padding=True,
            truncation=True,
            return_tensors="pt",
            max_length=8192
        )
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model(**inputs)
            pooled = self.average_pool(outputs.last_hidden_state, inputs["attention_mask"])
            embedding = F.normalize(pooled, p=2, dim=1)

        return embedding[0]


    def embed_query(self, text: str) -> List[float]:
        embedding = self._embed_text(text, prefix="query")
        return embedding.cpu().tolist()