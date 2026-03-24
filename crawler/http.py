from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, Optional

import requests

from .rules import APIEndpoint, ThrottleRule


class HttpClient:
    """带节流/重试的简单HTTP客户端（中文注释）。"""

    def __init__(self, throttle: ThrottleRule) -> None:
        self._throttle = throttle
        self._session = requests.Session()

    def fetch_json(self, endpoint: APIEndpoint, extra_params: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """按规则发起GET请求并返回JSON（中文注释）。"""
        params = dict(endpoint.default_params)
        params.update(extra_params)
        return self._request(endpoint.url, params, headers)

    def _request(self, url: str, params: Dict[str, Any], headers: Optional[Dict[str, str]]) -> Dict[str, Any]:
        """包含指数退避重试的底层请求逻辑（中文注释）。"""
        backoff = 1.0
        last_error: Optional[Exception] = None
        for attempt in range(1, self._throttle.max_retries + 1):
            self._sleep_once()
            response: Optional[requests.Response] = None
            try:
                response = self._session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self._throttle.timeout,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_error = exc
                logging.warning(
                    "HTTP request failed (attempt %s/%s): %s",
                    attempt,
                    self._throttle.max_retries,
                    exc,
                )
            except ValueError as exc:
                status = response.status_code if response is not None else "-"
                preview = (response.text or "")[:200] if response is not None else ""
                error = RuntimeError(
                    "Failed to parse JSON from %s (status=%s): %s"
                    % (url, status, preview.replace("\n", " "))
                )
                last_error = error
                logging.warning(
                    "HTTP JSON decode failed (attempt %s/%s, status=%s): %s",
                    attempt,
                    self._throttle.max_retries,
                    status,
                    preview.strip(),
                )
            if attempt == self._throttle.max_retries:
                if last_error:
                    raise last_error
                raise RuntimeError("Retry loop exhausted without raising")
            time.sleep(backoff)
            backoff *= self._throttle.retry_backoff
        if last_error:
            raise last_error
        raise RuntimeError("Retry loop exhausted without raising")

    def _sleep_once(self) -> None:
        """根据 throttle 范围随机延迟，降低压力（中文注释）。"""
        delay = random.uniform(self._throttle.min_seconds, self._throttle.max_seconds)
        time.sleep(delay)

    def close(self) -> None:
        """关闭底层Session（中文注释）。"""
        self._session.close()

