"""Client for the ipwatch-embed service (anferico/bert-for-patents, 768d)."""
import logging
from typing import Optional
import httpx
from app.config import settings

logger = logging.getLogger(__name__)

_client: Optional["EmbedClient"] = None


class EmbedClient:
    def __init__(self, base_url: str):
        self._url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(timeout=30.0)

    async def embed(self, text: str) -> Optional[list[float]]:
        """Embed a single text. Returns None if service unavailable."""
        try:
            r = await self._http.post(f"{self._url}/embed", json={"text": text})
            r.raise_for_status()
            return r.json()["vector"]
        except Exception as e:
            logger.warning(f"Embed service unavailable: {e}")
            return None

    async def embed_batch(self, texts: list[str]) -> list[Optional[list[float]]]:
        """Embed a list of texts. Returns list of None on failure."""
        try:
            r = await self._http.post(f"{self._url}/embed-batch", json={"texts": texts})
            r.raise_for_status()
            return r.json()["vectors"]
        except Exception as e:
            logger.warning(f"Embed batch service unavailable: {e}")
            return [None] * len(texts)

    async def close(self):
        await self._http.aclose()


def get_embed_client() -> Optional[EmbedClient]:
    return _client


def init_embed_client():
    global _client
    url = settings.embed_service_url
    if url:
        _client = EmbedClient(url)
        logger.info(f"Embed client initialized → {url}")
    else:
        logger.info("EMBED_SERVICE_URL not set — semantic search disabled")
