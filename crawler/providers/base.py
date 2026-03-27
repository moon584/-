from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from crawler.models import JobRecord
from crawler.rules import APIEndpoint, CompanyRule


@dataclass(slots=True)
class ListResult:
    """列表接口返回的标准结构，便于不同官网共用调度逻辑。"""

    posts: List[Dict[str, Any]]
    total_count: Optional[int]
    has_more: bool


class BaseProvider:
    """官网适配器基类，子类只需关心字段映射和接口差异。"""

    def __init__(self, rule: CompanyRule) -> None:
        self.rule = rule
        self.extra = rule.extra

    @property
    def company_id(self) -> str:
        return self.rule.company_id

    @property
    def list_endpoint(self) -> APIEndpoint:
        return self.rule.list_api

    @property
    def detail_endpoint(self) -> APIEndpoint:
        return self.rule.detail_api

    def list_headers(self) -> Optional[Dict[str, str]]:
        return None

    def detail_headers(self) -> Optional[Dict[str, str]]:
        return None

    def warmup_urls(self) -> List[str]:
        """返回预热URL列表，用于请求前建立会话（如站点种cookie）。"""

        return []

    def warmup_headers(self) -> Optional[Dict[str, str]]:
        return None

    # ---- 列表阶段 ----
    def build_list_params(self, category_id: str, page: int) -> Dict[str, Any]:
        """构造列表接口参数，默认直接沿用 rule 中的默认值。"""

        params = dict(self.list_endpoint.default_params)
        params.update({"pageIndex": page})
        if category_id:
            params["categoryId"] = category_id
        return params

    def parse_list_response(self, payload: Dict[str, Any], page: int) -> ListResult:
        """解析官网响应，返回标准化的列表结果。"""

        raise NotImplementedError

    def extract_post_id(self, post: Dict[str, Any]) -> Optional[str]:
        """从单条列表数据中提取唯一的岗位标识。"""

        raise NotImplementedError

    def predict_job_url(self, post: Dict[str, Any]) -> Optional[str]:
        """根据列表数据预估 job_url；默认不支持。"""

        return None

    # ---- 详情阶段 ----
    def build_detail_params(self, post_id: str) -> Dict[str, Any]:
        params = dict(self.detail_endpoint.default_params)
        params.update({"postId": post_id})
        return params

    def parse_detail_response(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """检查详情响应并返回业务数据。"""

        raise NotImplementedError

    # ---- 映射阶段 ----
    def build_job_record(self, category_id: str, detail: Dict[str, Any], *, crawled_at: datetime) -> JobRecord:
        """将详情 JSON 转为 JobRecord，由各官网自行实现映射逻辑。"""

        raise NotImplementedError

    # ---- 自动分类扩展 ----
    def supports_auto_category(self) -> bool:
        """是否支持基于 JSON 自动推断分类。默认关闭。"""

        return False

    def resolve_category_id(self, post: Dict[str, Any], detail: Dict[str, Any]) -> Optional[str]:
        """根据岗位 JSON 解析数据库分类ID；默认不覆盖。"""

        return None

