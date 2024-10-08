from pydantic_settings import BaseSettings


class DatabaseConfig(BaseSettings):
    POSTGRES_HOST: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_PORT: int = 5432
