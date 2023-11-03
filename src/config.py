from pydantic import Field, AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict


class DBSettings(BaseSettings):
    """ Конфигратор настроек для БД. """

    _db_env_prefix = "DB_"

    DRIVERNAME: str = "postgresql+asyncpg"
    HOST: str
    PORT: int
    DATABASE: str = Field(
        validation_alias=AliasChoices(f"{_db_env_prefix}NAME", f"{_db_env_prefix}DATABASE")
    )
    USERNAME: str = Field(
        validation_alias=AliasChoices(f"{_db_env_prefix}USER", f"{_db_env_prefix}USERNAME")
    )
    PASSWORD: str = Field(
        validation_alias=AliasChoices("POSTGRES_PASSWORD", f"{_db_env_prefix}PASSWORD")
    )
    model_config = SettingsConfigDict(
        case_sensitive=True,
        env_prefix=_db_env_prefix,
        extra="ignore",
    )

    @property
    def connection_string(self) -> str:
        return f"{self.USERNAME}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"

db_settings = DBSettings()
