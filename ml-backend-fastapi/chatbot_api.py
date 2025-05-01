# chatbot_api.py
from fastapi import FastAPI, Request
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# CORS 허용 (스프링 서버에서 호출 가능하게)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 혹은 ["http://localhost:8080"] 등으로 제한
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Query(BaseModel):
    message: str

# 사용자가 정의한 답변 함수
def answer_question(query: str) -> str:
    return f"'{query}'에 대한 답변입니다."  # 실제 구현으로 교체

@app.post("/api/chatbot")
async def chatbot_endpoint(query: Query):
    answer = answer_question(query.message)
    return {"reply": answer}