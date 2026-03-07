from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from typing import Any

from redis.exceptions import RedisError

from compliance_gate.authentication.config import auth_settings
from compliance_gate.infra.cache.redis import get_redis


@dataclass(slots=True)
class _MemoryItem:
    value: Any
    expires_at: float


class AuthLimiter:
    def __init__(self) -> None:
        self._memory: dict[str, _MemoryItem] = {}

    def _hash(self, value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _redis(self):
        return get_redis()

    def _mem_get(self, key: str) -> Any | None:
        item = self._memory.get(key)
        if not item:
            return None
        if item.expires_at < time.time():
            self._memory.pop(key, None)
            return None
        return item.value

    def _mem_set(self, key: str, value: Any, ttl: int) -> None:
        self._memory[key] = _MemoryItem(value=value, expires_at=time.time() + ttl)

    def _mem_delete(self, key: str) -> None:
        self._memory.pop(key, None)

    def _set(self, key: str, value: str, ttl: int) -> None:
        try:
            self._redis().setex(key, ttl, value)
        except RedisError:
            self._mem_set(key, value, ttl)

    def _get(self, key: str) -> Any | None:
        try:
            return self._redis().get(key)
        except RedisError:
            return self._mem_get(key)

    def _delete(self, key: str) -> None:
        try:
            self._redis().delete(key)
        except RedisError:
            self._mem_delete(key)

    def _incr(self, key: str, ttl: int) -> int:
        try:
            client = self._redis()
            value = client.incr(key)
            if value == 1:
                client.expire(key, ttl)
            return int(value)
        except RedisError:
            current = self._mem_get(key)
            next_value = int(current or 0) + 1
            self._mem_set(key, next_value, ttl)
            return next_value

    def _failure_key(self, scope: str, value: str) -> str:
        return f"auth:fail:{scope}:{self._hash(value.lower())}"

    def _lock_key(self, scope: str, value: str) -> str:
        return f"auth:lock:{scope}:{self._hash(value.lower())}"

    def is_locked(self, username: str, ip_address: str | None) -> bool:
        user_lock = self._get(self._lock_key("user", username))
        if user_lock:
            return True
        if ip_address:
            ip_lock = self._get(self._lock_key("ip", ip_address))
            if ip_lock:
                return True
        return False

    def register_login_failure(self, username: str, ip_address: str | None) -> bool:
        locked = False
        user_fails = self._incr(
            self._failure_key("user", username),
            auth_settings.auth_rate_limit_window_seconds,
        )
        if user_fails >= auth_settings.auth_rate_limit_max_attempts:
            self._set(
                self._lock_key("user", username),
                "1",
                auth_settings.auth_lock_seconds,
            )
            locked = True

        if ip_address:
            ip_fails = self._incr(
                self._failure_key("ip", ip_address),
                auth_settings.auth_rate_limit_window_seconds,
            )
            if ip_fails >= auth_settings.auth_rate_limit_max_attempts:
                self._set(
                    self._lock_key("ip", ip_address),
                    "1",
                    auth_settings.auth_lock_seconds,
                )
                locked = True

        return locked

    def clear_login_failures(self, username: str, ip_address: str | None) -> None:
        self._delete(self._failure_key("user", username))
        self._delete(self._lock_key("user", username))
        if ip_address:
            self._delete(self._failure_key("ip", ip_address))
            self._delete(self._lock_key("ip", ip_address))

    def store_login_challenge(self, challenge_id: str, user_id: str) -> None:
        self._set(
            f"auth:challenge:{challenge_id}",
            user_id,
            auth_settings.auth_login_challenge_ttl_seconds,
        )

    def consume_login_challenge(self, challenge_id: str) -> str | None:
        key = f"auth:challenge:{challenge_id}"
        user_id = self._get(key)
        self._delete(key)
        return str(user_id) if user_id else None

    def store_pending_mfa_secret(self, user_id: str, secret: str) -> None:
        self._set(
            f"auth:mfa:pending:{user_id}",
            secret,
            auth_settings.auth_mfa_setup_ttl_seconds,
        )

    def get_pending_mfa_secret(self, user_id: str) -> str | None:
        value = self._get(f"auth:mfa:pending:{user_id}")
        return str(value) if value else None

    def clear_pending_mfa_secret(self, user_id: str) -> None:
        self._delete(f"auth:mfa:pending:{user_id}")


auth_limiter = AuthLimiter()
