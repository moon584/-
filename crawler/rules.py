from __future__ import annotations

from dataclasses import dataclass, replace
from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class APIEndpoint:
    """接口地址及默认参数，便于统一调用（中文注释）。"""

    url: str
    default_params: Dict[str, Any]
    method: str = "GET"


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


def apply_job_type_overrides(rule: CompanyRule, job_type: int) -> CompanyRule:
    overrides = rule.extra.get("job_type_overrides") if isinstance(rule.extra, dict) else None
    if not isinstance(overrides, dict):
        return rule
    variant = overrides.get(str(job_type))
    if not isinstance(variant, dict):
        return rule
    new_list = _merge_endpoint(rule.list_api, variant.get("list_api"))
    new_detail = _merge_endpoint(rule.detail_api, variant.get("detail_api"))
    new_extra = _merge_dict(rule.extra, variant.get("extra"))
    return replace(rule, list_api=new_list, detail_api=new_detail, extra=new_extra)


def _merge_endpoint(base: APIEndpoint, override: Optional[Dict[str, Any]]) -> APIEndpoint:
    if not isinstance(override, dict):
        return base
    url = override.get("url", base.url)
    method = override.get("method", base.method)
    default_params = dict(base.default_params)
    override_params = override.get("default_params")
    if isinstance(override_params, dict):
        default_params.update(override_params)
    return APIEndpoint(url=url, default_params=default_params, method=method)


def _merge_dict(base: Dict[str, Any], patch: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = deepcopy(base)
    if not isinstance(patch, dict):
        return merged

    def _apply(target: Dict[str, Any], source: Dict[str, Any]) -> None:
        for key, value in source.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                _apply(target[key], value)
            else:
                target[key] = value

    _apply(merged, patch)
    return merged

