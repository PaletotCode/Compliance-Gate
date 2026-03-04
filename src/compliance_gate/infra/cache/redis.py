import redis
from compliance_gate.config.settings import settings

redis_client = redis.from_url(
    settings.redis_url,
    decode_responses=True
)

def get_redis():
    return redis_client
