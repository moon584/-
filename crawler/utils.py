from __future__ import annotations

from datetime import datetime
from typing import Optional


SUPPORTED_TIME_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%Y年%m月%d日",
)


def parse_publish_time(raw_value: Optional[str]) -> Optional[datetime]:
    """解析多种官网时间格式（中文注释）。"""
    if not raw_value:
        return None
    sanitized = raw_value.strip()
    for pattern in SUPPORTED_TIME_FORMATS:
        try:
            parsed = datetime.strptime(sanitized, pattern)
            if "H" not in pattern:
                return parsed.replace(hour=0, minute=0, second=0)
            return parsed
        except ValueError:
            continue
    return None


def normalize_category_id(raw_value: str) -> str:
    """修正数据库中的 categoryid（某些导入会缺少 0），确保符合腾讯接口格式。"""
    if raw_value is None:
        raise ValueError("categoryid 不能为空")
    text = str(raw_value).strip()
    if not text:
        raise ValueError("categoryid 不能为空字符串")
    if len(text) == 7:
        text = f"{text[:3]}0{text[3:]}"
    if len(text) != 8 or not text.isdigit():
        raise ValueError("categoryid 必须为 8 位数字")
    return text
