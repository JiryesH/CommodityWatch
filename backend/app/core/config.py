from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="CW_",
        extra="ignore",
        case_sensitive=False,
    )

    env: str = "development"
    base_url: str = "http://localhost:8000"
    cors_allowed_origins_raw: str = "http://127.0.0.1:3000,http://localhost:3000,http://127.0.0.1:8080,http://localhost:8080"
    log_level: str = "INFO"
    secret_key: SecretStr = SecretStr("change-me")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/commoditywatch"

    session_cookie_name: str = "cw_session"
    session_cookie_domain: str | None = None
    session_max_age_seconds: int = 60 * 60 * 24 * 30
    session_csrf_cookie_name: str = "cw_csrf"

    stripe_secret_key: SecretStr | None = None
    stripe_webhook_secret: SecretStr | None = None
    stripe_price_pro_monthly: str | None = None
    stripe_portal_configuration_id: str | None = None

    eia_api_key: SecretStr | None = None
    agsi_api_key: SecretStr | None = None
    usda_nass_api_key: SecretStr | None = None
    fred_api_key: SecretStr | None = None
    ember_api_key: SecretStr | None = None
    cds_api_key: SecretStr | None = None
    cds_uid: str | None = None

    artifact_root: Path = Field(default=Path("./artifacts"))
    artifact_s3_bucket: str | None = None
    artifact_s3_endpoint: str | None = None

    alert_webhook_url: str | None = None

    snapshot_ttl_seconds: int = 300
    worker_heartbeat_grace_seconds: int = 7200
    eia_rate_limit_seconds: float = 1.0
    agsi_rate_limit_seconds: float = 1.1
    usda_psd_rate_limit_seconds: float = 0.5
    fred_rate_limit_seconds: float = 0.25
    ember_rate_limit_seconds: float = 0.25
    oecd_rate_limit_seconds: float = 0.25
    exchange_scrape_rate_limit_seconds: float = 1.0
    enable_lme_live_jobs: bool = False
    enable_ice_certified_jobs: bool = False
    auth_rate_limit_max_requests: int = 10
    auth_rate_limit_window_seconds: int = 900

    @property
    def sync_database_url(self) -> str:
        if self.database_url.startswith("postgresql+asyncpg://"):
            return self.database_url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
        return self.database_url

    @property
    def is_production(self) -> bool:
        return self.env.lower() == "production"

    @property
    def cors_allowed_origins(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.cors_allowed_origins_raw.split(",")
            if origin.strip()
        ]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.artifact_root.mkdir(parents=True, exist_ok=True)
    return settings
