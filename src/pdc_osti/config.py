from pydantic import BaseSettings


class Settings(BaseSettings):
    """API Configuration"""

    OSTI_USERNAME_TEST: str
    OSTI_PASSWORD_TEST: str
    OSTI_USERNAME_PROD: str
    OSTI_PASSWORD_PROD: str

    class Config:
        env_file = ".env"


settings = Settings()
