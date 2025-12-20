from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    app_env: str = "local"
    database_url: str
    jwt_secret: str
    jwt_expires_min: int = 240

    admin_username: str = "admin"
    admin_password: str = "admin"

    kafka_bootstrap: str = "localhost:9092"
    topic_classified: str = "events.classified"
    topic_in: str = "events.in"

    retention_days: int = 7


settings = Settings()
