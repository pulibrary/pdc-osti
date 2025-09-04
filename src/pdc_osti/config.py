from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """API Configuration"""

    ELINK2_TOKEN_TEST: str
    ELINK2_TOKEN_PROD: str

    class Config:
        env_file = ".env"


settings = Settings()
