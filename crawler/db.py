from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import Dict, Iterable, Iterator, List, Optional, Set

import pymysql
from pymysql.cursors import DictCursor

from .config import Settings
from .models import CategoryMapping
from .utils import normalize_category_id


class Database:
    """封装MySQL操作的轻量封装层（中文注释）。"""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._connection: Optional[pymysql.connections.Connection] = None

    def _ensure_connection(self) -> pymysql.connections.Connection:
        if self._connection is None or not self._connection.open:
            self._connection = pymysql.connect(
                host=self._settings.db_host,
                port=self._settings.db_port,
                user=self._settings.db_user,
                password=self._settings.db_password,
                database=self._settings.db_name,
                charset="utf8mb4",
                cursorclass=DictCursor,
                autocommit=False,
            )
        return self._connection

    @contextmanager
    def cursor(self) -> Iterator[DictCursor]:
        conn = self._ensure_connection()
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def fetch_category_mappings(
        self,
        company_id: str,
        *,
        category_ids: Optional[List[str]] = None,
        only_leaf: bool = True,
    ) -> List[CategoryMapping]:
        """按需返回分类映射，可选只取叶子节点或指定ID（中文注释：便于交互选择分类）。"""
        select_sql = "SELECT c.id, c.categoryid, c.crawled_job_count, c.official_job_count FROM category c"
        joins: List[str] = []
        conditions: List[str] = ["c.categoryid IS NOT NULL", "c.id LIKE %s"]
        params: List[object] = [f"{company_id}%"]
        if only_leaf:
            joins.append("LEFT JOIN category child ON child.parent_id = c.id")
            conditions.append("child.id IS NULL")
        if category_ids:
            placeholders = ",".join(["%s"] * len(category_ids))
            conditions.append(f"c.id IN ({placeholders})")
            params.extend(category_ids)
        query = " ".join([select_sql] + joins)
        query += " WHERE " + " AND ".join(conditions)
        with self.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
        mappings: List[CategoryMapping] = []
        for row in rows:
            try:
                normalized_id = normalize_category_id(row["categoryid"])
            except ValueError as exc:
                logging.error(
                    "分类 %s 的 categoryid '%s' 非法：%s",
                    row["id"],
                    row["categoryid"],
                    exc,
                )
                continue
            mappings.append(
                CategoryMapping(
                    db_category_id=row["id"],
                    api_category_id=normalized_id,
                    crawled_job_count=int(row.get("crawled_job_count") or 0),
                    official_job_count=int(row.get("official_job_count") or 0),
                )
            )
        return mappings

    def fetch_job_by_url(self, job_url: str) -> Optional[Dict[str, object]]:
        with self.cursor() as cur:
            cur.execute("SELECT * FROM job WHERE job_url=%s", (job_url,))
            row = cur.fetchone()
        return row

    def fetch_category_ids(self, company_id: str) -> Set[str]:
        """返回公司下所有分类ID（不要求 categoryid 字段）."""
        with self.cursor() as cur:
            cur.execute("SELECT id FROM category WHERE id LIKE %s", (f"{company_id}%",))
            rows = cur.fetchall()
        return {str(row["id"]) for row in rows if row.get("id")}

    def ensure_categories_exist(self, company_id: str, category_ids: Iterable[str]) -> List[str]:
        """批量创建缺失分类，返回本次新增的分类ID列表。"""
        normalized_ids = []
        seen: Set[str] = set()
        for raw in category_ids:
            cid = str(raw).strip()
            if not cid or cid in seen:
                continue
            seen.add(cid)
            normalized_ids.append(cid)
        if not normalized_ids:
            return []

        existing_ids = self.fetch_category_ids(company_id)
        missing_ids = [cid for cid in normalized_ids if cid not in existing_ids]
        if not missing_ids:
            return []

        with self.cursor() as cur:
            for category_id in missing_ids:
                # 自动补齐一级分类节点，名称先使用ID，后续可在管理端调整。
                cur.execute(
                    "INSERT INTO category (id, name, parent_id, level, categoryid) VALUES (%s, %s, %s, %s, %s)",
                    (category_id, category_id, company_id, 0, None),
                )
        self._ensure_connection().commit()
        return missing_ids

    def generate_next_job_id(self, company_id: str) -> str:
        with self.cursor() as cur:
            cur.execute(
                "SELECT id FROM job WHERE company_id=%s ORDER BY id",
                (company_id,),
            )
            rows = cur.fetchall()
        existing_ids = [row["id"] for row in rows]
        return self._compute_next_job_id(company_id, existing_ids)

    @staticmethod
    def _compute_next_job_id(company_id: str, existing_ids: List[str]) -> str:
        prefix = f"{company_id}J"
        expected = 1
        for job_id in sorted(existing_ids):
            suffix = Database._extract_suffix(job_id, prefix)
            if suffix is None:
                continue
            if suffix > expected:
                break
            if suffix == expected:
                expected += 1
        return f"{company_id}J{expected:05d}"

    @staticmethod
    def _extract_suffix(job_id: str, prefix: str) -> Optional[int]:
        if not job_id.startswith(prefix):
            return None
        remainder = job_id[len(prefix) :]
        try:
            return int(remainder)
        except (TypeError, ValueError):
            return None

    def insert_job(self, job_values: Dict[str, object]) -> None:
        columns = ",".join(job_values.keys())
        placeholders = ",".join(["%s"] * len(job_values))
        sql = f"INSERT INTO job ({columns}) VALUES ({placeholders})"
        with self.cursor() as cur:
            cur.execute(sql, tuple(job_values.values()))
        self._ensure_connection().commit()

    def update_job(self, job_id: str, changes: Dict[str, object]) -> None:
        if not changes:
            return
        assignments = ",".join([f"{col}=%s" for col in changes.keys()])
        sql = f"UPDATE job SET {assignments} WHERE id=%s"
        params = list(changes.values()) + [job_id]
        with self.cursor() as cur:
            cur.execute(sql, params)
        self._ensure_connection().commit()

    def count_jobs_in_category(self, category_id: str) -> int:
        """统计指定分类当前已写入的职位数量。"""
        with self.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM job WHERE category_id=%s", (category_id,))
            row = cur.fetchone() or {"total": 0}
        return int(row["total"] or 0)

    def delete_jobs_by_category(self, category_id: str) -> int:
        """删除指定分类下的所有职位记录，返回受影响行数。"""
        with self.cursor() as cur:
            cur.execute("DELETE FROM job WHERE category_id=%s", (category_id,))
            deleted = cur.rowcount or 0
        self._ensure_connection().commit()
        return deleted

    def mark_jobs_deleted_by_category(self, category_id: str) -> int:
        """慢爬准备阶段：先将分类下职位标记为待删除。"""
        with self.cursor() as cur:
            cur.execute("UPDATE job SET is_deleted=1 WHERE category_id=%s", (category_id,))
            affected = cur.rowcount or 0
        self._ensure_connection().commit()
        return affected

    def mark_jobs_deleted_by_company(self, company_id: str) -> int:
        """慢爬自动分类准备阶段：先将公司下职位标记为待删除。"""
        with self.cursor() as cur:
            cur.execute("UPDATE job SET is_deleted=1 WHERE company_id=%s", (company_id,))
            affected = cur.rowcount or 0
        self._ensure_connection().commit()
        return affected

    def touch_job_alive_by_url(self, job_url: str) -> bool:
        """命中列表中的岗位即视为存活，取消待删除标记。"""
        with self.cursor() as cur:
            cur.execute("UPDATE job SET is_deleted=0 WHERE job_url=%s", (job_url,))
            affected = cur.rowcount or 0
        self._ensure_connection().commit()
        return affected > 0

    def purge_deleted_jobs_by_category(self, category_id: str) -> int:
        """慢爬收尾：删除仍处于待删除标记的职位。"""
        with self.cursor() as cur:
            cur.execute("DELETE FROM job WHERE category_id=%s AND is_deleted=1", (category_id,))
            deleted = cur.rowcount or 0
        self._ensure_connection().commit()
        return deleted

    def purge_deleted_jobs_by_company(self, company_id: str) -> int:
        """慢爬自动分类收尾：删除公司下仍处于待删除标记的职位。"""
        with self.cursor() as cur:
            cur.execute("DELETE FROM job WHERE company_id=%s AND is_deleted=1", (company_id,))
            deleted = cur.rowcount or 0
        self._ensure_connection().commit()
        return deleted

    def clear_deleted_marks_by_category(self, category_id: str) -> int:
        """慢爬异常回滚：取消分类下待删除标记，避免误删。"""
        with self.cursor() as cur:
            cur.execute("UPDATE job SET is_deleted=0 WHERE category_id=%s AND is_deleted=1", (category_id,))
            affected = cur.rowcount or 0
        self._ensure_connection().commit()
        return affected

    def clear_deleted_marks_by_company(self, company_id: str) -> int:
        """慢爬异常回滚：取消公司下待删除标记，避免误删。"""
        with self.cursor() as cur:
            cur.execute("UPDATE job SET is_deleted=0 WHERE company_id=%s AND is_deleted=1", (company_id,))
            affected = cur.rowcount or 0
        self._ensure_connection().commit()
        return affected

    def sync_category_counts(self, category_id: str, official_total: Optional[int] = None) -> int:
        """同步分类的爬取/官网职位数量，返回最新 crawled 数量。"""
        crawled_total = self.count_jobs_in_category(category_id)
        with self.cursor() as cur:
            if official_total is None:
                cur.execute(
                    "UPDATE category SET crawled_job_count=%s WHERE id=%s",
                    (crawled_total, category_id),
                )
            else:
                cur.execute(
                    "UPDATE category SET crawled_job_count=%s, official_job_count=%s WHERE id=%s",
                    (crawled_total, max(official_total, 0), category_id),
                )
        self._ensure_connection().commit()
        return crawled_total

    def rollback(self) -> None:
        conn = self._ensure_connection()
        conn.rollback()

    def close(self) -> None:
        if self._connection and self._connection.open:
            self._connection.close()
            self._connection = None

