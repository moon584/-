"""Provider registry，用于按官网类型选择不同的字段解析逻辑。"""

from __future__ import annotations

from typing import Dict, Type

from crawler.rules import CompanyRule

from .base import BaseProvider
from .config_provider import ConfigDrivenProvider

_PROVIDER_REGISTRY: Dict[str, Type[BaseProvider]] = {
    "config": ConfigDrivenProvider,
    "tencent": ConfigDrivenProvider,
}


def load_provider(name: str, rule: CompanyRule) -> BaseProvider:
    key = name.lower().strip()
    if key not in _PROVIDER_REGISTRY:
        available = ", ".join(sorted(_PROVIDER_REGISTRY.keys()))
        raise ValueError(f"未知 provider '{name}'，可选值：{available}")
    provider_cls = _PROVIDER_REGISTRY[key]
    return provider_cls(rule)
