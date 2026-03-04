from datetime import datetime
from zoneinfo import ZoneInfo

DEFAULT_TZ = ZoneInfo("America/Sao_Paulo")

def now() -> datetime:
    return datetime.now(DEFAULT_TZ)
