from celery import Celery
from compliance_gate.config.settings import settings

celery_app = Celery(
    "compliance_gate",
    broker=settings.redis_url,
    backend=settings.redis_url
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="America/Sao_Paulo",
    enable_utc=True,
)
