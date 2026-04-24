from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://ipwatch:password@localhost:5432/patent_db"
    jwt_secret_key: str = "change-me-in-production"
    openrouter_api_key: str = ""
    hf_home: str = ""
    cors_origins: str = "*"
    embed_service_url: str = ""  # http://ipwatch-embed:8002 when embed container running

    class Config:
        env_file = ".env"
        case_sensitive = False


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
