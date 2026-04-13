"""
OpenRouter LLM client via openai SDK (base_url override).
Used only for AI-enriched features (keyword search enrichment, etc.).
"""
from openai import AsyncOpenAI
from app.config import settings

_client: AsyncOpenAI | None = None


def get_llm() -> AsyncOpenAI:
    global _client
    if _client is None:
        _client = AsyncOpenAI(
            api_key=settings.openrouter_api_key,
            base_url="https://openrouter.ai/api/v1",
            default_headers={
                "HTTP-Referer": "https://squarkip.com",
                "X-Title": "IPWatch",
            },
        )
    return _client
