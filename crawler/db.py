from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import logging
from typing import Dict, Iterator, List, Optional

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
        select_sql = "SELECT c.id, c.categoryid FROM category c"
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
                CategoryMapping(db_category_id=row["id"], api_category_id=normalized_id)
            )
        return mappings

    def fetch_job_by_url(self, job_url: str) -> Optional[Dict[str, object]]:
        with self.cursor() as cur:
            cur.execute("SELECT * FROM job WHERE job_url=%s", (job_url,))
            row = cur.fetchone()
        return row

    def generate_next_job_id(self, company_id: str) -> str:
        with self.cursor() as cur:
            cur.execute(
                "SELECT id FROM job WHERE company_id=%s ORDER BY id DESC LIMIT 1",
                (company_id,),
            )
            row = cur.fetchone()
        if not row:
            return f"{company_id}J00001"
        suffix = int(row["id"][-5:]) + 1
        return f"{company_id}J{suffix:05d}"

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

    def rollback(self) -> None:
        conn = self._ensure_connection()
        conn.rollback()

    def close(self) -> None:
        if self._connection and self._connection.open:
            self._connection.close()
            self._connection = None

