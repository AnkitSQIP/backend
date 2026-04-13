"""
Patent embedding service using AI-Growth-Lab/PatentSBERTa.
768-dim, ~250MB VRAM, patent-specific (trained on 1.5M claims).
GPU-first: uses CUDA if available, falls back to CPU.
"""
import logging
from typing import List

logger = logging.getLogger(__name__)


class EmbeddingService:
    def __init__(self):
        self._model = None
        self._device = None

    def _load(self):
        if self._model is not None:
            return
        try:
            import torch
            from sentence_transformers import SentenceTransformer

            self._device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Loading PatentSBERTa on {self._device}")
            self._model = SentenceTransformer("AI-Growth-Lab/PatentSBERTa", device=self._device)
            logger.info("PatentSBERTa loaded successfully")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            raise

    async def embed(self, text: str) -> List[float]:
        self._load()
        text = text.replace("\n", " ").strip()
        if not text:
            return [0.0] * 768
        return self._model.encode(text, normalize_embeddings=True).tolist()

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        self._load()
        texts = [t.replace("\n", " ").strip() for t in texts]
        bs = 64 if self._device == "cuda" else 16
        return self._model.encode(texts, normalize_embeddings=True, batch_size=bs).tolist()


embedding_service = EmbeddingService()
