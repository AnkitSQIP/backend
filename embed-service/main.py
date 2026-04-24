"""
Patent embedding service — anferico/bert-for-patents (768d)
POST /embed       → single text → vector
POST /embed-batch → list of texts → list of vectors
"""
import os
import logging
from contextlib import asynccontextmanager
from typing import Optional
import torch
from transformers import AutoTokenizer, AutoModel
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_NAME = "anferico/bert-for-patents"
MODEL_CACHE = os.environ.get("MODEL_CACHE", "/app/models")
MAX_LENGTH = 512
BATCH_SIZE = 32

_tokenizer = None
_model = None


def _load_model():
    global _tokenizer, _model
    logger.info(f"Loading {MODEL_NAME}...")
    _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, cache_dir=MODEL_CACHE)
    _model = AutoModel.from_pretrained(MODEL_NAME, cache_dir=MODEL_CACHE)
    _model.eval()
    logger.info("Model loaded")


def _mean_pool(token_embeddings, attention_mask):
    mask = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * mask, 1) / torch.clamp(mask.sum(1), min=1e-9)


def _embed_texts(texts: list[str]) -> list[list[float]]:
    encoded = _tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )
    with torch.no_grad():
        output = _model(**encoded)
    embeddings = _mean_pool(output.last_hidden_state, encoded["attention_mask"])
    # L2 normalize
    norms = embeddings.norm(dim=1, keepdim=True).clamp(min=1e-9)
    normalized = (embeddings / norms).tolist()
    return normalized


@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_model()
    yield


app = FastAPI(title="IPWatch Embed Service", lifespan=lifespan)


class EmbedRequest(BaseModel):
    text: str


class EmbedBatchRequest(BaseModel):
    texts: list[str]


@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL_NAME}


@app.post("/embed")
def embed(req: EmbedRequest):
    vector = _embed_texts([req.text])[0]
    return {"vector": vector}


@app.post("/embed-batch")
def embed_batch(req: EmbedBatchRequest):
    if not req.texts:
        return {"vectors": []}
    # Process in batches to avoid OOM on large requests
    all_vectors = []
    for i in range(0, len(req.texts), BATCH_SIZE):
        batch = req.texts[i:i + BATCH_SIZE]
        all_vectors.extend(_embed_texts(batch))
    return {"vectors": all_vectors}
