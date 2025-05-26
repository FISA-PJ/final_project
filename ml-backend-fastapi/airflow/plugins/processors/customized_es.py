import uuid
import hashlib
from langchain.schema import Document
from langchain_core.stores import BaseStore
from elasticsearch import Elasticsearch
from langchain_elasticsearch import ElasticsearchStore
from typing import Iterator, Optional

class ElasticsearchDocstore(BaseStore):
    def __init__(self, es: Elasticsearch, index_name: str):
        self.es = es
        self.index = index_name

        # 인덱스 없으면 생성
        if not self.es.indices.exists(index=self.index):
            self.es.indices.create(index=self.index, body={
                "mappings": {
                    "properties": {
                        "content": {"type": "text"},
                        "metadata": {"type": "object"}
                    }
                }
            })

    def mget(self, keys: list[str]) -> dict:
        response = self.es.mget(index=self.index, body={"ids": keys})
        return {
            doc["_id"]: {
                "content": doc["_source"]["content"],
                "metadata": doc["_source"].get("metadata", {})
            }
            for doc in response["docs"] if doc["found"]
        }

    def mset(self, docs: list[tuple[str, Document]]) -> None:
        for doc_id, doc in docs:
            print(doc_id)
            doc_id = doc.metadata.get("doc_id")
            print(doc_id)
            body = {
                "content": doc.page_content,
                "metadata": doc.metadata
            }
            self.es.index(index=self.index, id=doc_id, document=body)

    def delete(self, key: str) -> None:
        if self.es.exists(index=self.index, id=key):
            self.es.delete(index=self.index, id=key)

    def mdelete(self, keys: list[str]) -> None:
        for key in keys:
            self.delete(key)

    def yield_keys(self, *, prefix: Optional[str] = None) -> Iterator[str]:
        query = {"match_all": {}} if prefix is None else {
            "prefix": {"_id": prefix}
        }

        page = self.es.search(
            index=self.index,
            scroll='2m',
            size=1000,
            body={"query": query, "_source": False}
        )

        scroll_id = page['_scroll_id']
        hits = page['hits']['hits']

        while hits:
            for doc in hits:
                yield doc['_id']

            page = self.es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = page['_scroll_id']
            hits = page['hits']['hits']
            
    # def _get_doc_id(self, doc: Document) -> str:
    #     # UUID 기반 고유 ID 생성
    #     return str(uuid.uuid4())
    
class CustomElasticsearchStore(ElasticsearchStore):
    def add_texts(self, texts, metadatas=None, ids=None, **kwargs):
        # ID를 명시하지 않았을 경우 자동 생성
        if ids is None:
            ids = []
            for i, _ in enumerate(texts):
                meta = metadatas[i] if metadatas else {}
                hash_id = meta.get("child_id", "")
                # base = text + apt_code
                # hash_id = hashlib.sha256(base.encode("utf-8")).hexdigest()
                ids.append(hash_id)
                print("========",{hash_id})
            print(f"=========text==================\n{texts}\n=========meta==================\n{metadatas}\n=============id====\n{ids}")
        # 원래의 add_texts 로직 사용 (ids 포함)
        return super().add_texts(texts=texts, metadatas=metadatas, ids=ids, **kwargs)