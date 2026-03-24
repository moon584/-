from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, List


@dataclass(slots=True)
class APIEndpoint:
    """接口地址及默认参数，便于统一调用（中文注释）。"""

    url: str
    default_params: Dict[str, Any]


@dataclass(slots=True)
class ThrottleRule:
    """限速及重试策略（中文注释）。"""

    min_seconds: float
    max_seconds: float
    max_retries: int
    retry_backoff: float
    timeout: int


@dataclass(slots=True)
class CompanyRule:
    """某公司的爬虫规则（中文注释）。"""

    company_id: str
    company_name: str
    provider: str
    list_api: APIEndpoint
    detail_api: APIEndpoint
    throttle: ThrottleRule
    extra: Dict[str, Any]


def load_rule_file(path: str, company_id: str) -> CompanyRule:
    """读取规则JSON（可为数组），按 company_id 返回匹配配置。"""

    rule_path = Path(path)
    with rule_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    company_list: List[Dict[str, Any]]
    if isinstance(payload, list):
        company_list = payload
    else:
        company_list = [payload]
    target_id = company_id.strip().upper()
    for item in company_list:
        if str(item.get("company_id", "")).upper() != target_id:
            continue
        list_api = APIEndpoint(**item["list_api"])
        detail_api = APIEndpoint(**item["detail_api"])
        throttle = ThrottleRule(**item["throttle"])
        return CompanyRule(
            company_id=item["company_id"],
            company_name=item["company_name"],
            provider=item.get("provider", "tencent"),
            list_api=list_api,
            detail_api=detail_api,
            throttle=throttle,
            extra=item.get("extra", {}),
        )
    raise ValueError(f"规则文件 {path} 中找不到 company_id={company_id} 的配置")
