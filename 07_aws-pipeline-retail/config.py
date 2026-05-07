from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    aws_access_key_id: SecretStr = Field(...)
    aws_secret_access_key: SecretStr = Field(...)
    
    model_config = SettingsConfigDict()
    

settings = Settings()
