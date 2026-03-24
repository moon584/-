from __future__ import annotations

from datetime import datetime
import logging
from typing import Dict, List, Optional

from .db import Database
from .http import HttpClient
from .models import CategoryMapping, CrawlStats, JobRecord
from .providers.base import BaseProvider
from .utils import normalize_category_id


class JobCrawler:
    """通用抓取调度器，具体字段映射由 provider 决定。"""

    def __init__(
        self,
        db: Database,
        http_client: HttpClient,
        provider: BaseProvider,
        job_type: int,
        *,
        dry_run: bool = False,
    ) -> None:
        self._db = db
        self._http = http_client
        self._provider = provider
        self._job_type = job_type
        self._dry_run = dry_run
        self._stats: Optional[CrawlStats] = None

    def run(
        self,
        target_categories: Optional[List[str]] = None,
        post_limit: Optional[int] = None,
    ) -> CrawlStats:
        """执行爬取，支持指定分类和条数限制（中文注释）。"""
        stats = CrawlStats()
        self._stats = stats
        category_mappings = self._resolve_category_mappings(target_categories)
        if not category_mappings:
            logging.warning("指定的分类未找到或没有可爬取的叶子节点")
            return stats
        for mapping in category_mappings:
            api_category_id = mapping.api_category_id or ""
            logging.info(
                "开始抓取分类 %s（接口ID=%s，条数限制=%s）",
                mapping.db_category_id,
                mapping.api_category_id or "-",
                post_limit or "all",
            )
            posts = self._fetch_posts(api_category_id, post_limit)
            stats.record_category(mapping.db_category_id, len(posts))
            for post in posts:
                post_id = self._provider.extract_post_id(post)
                if not post_id:
                    stats.record_failure()
                    logging.warning("跳过缺少PostId的岗位：%s", post)
                    continue
                try:
                    detail = self._fetch_detail(post_id)
                    record = self._build_job_record(mapping.db_category_id, detail)
                    self._persist_record(record)
                    stats.record_success()
                except Exception:
                    stats.record_failure()
                    logging.exception("抓取岗位 %s 失败", post_id)
        logging.info(
            "分类抓取完成：共处理%s条，成功%s条，失败%s条",
            stats.total_posts,
            stats.success,
            stats.failed,
        )
        logging.info("各分类抓取数量：%s", stats.per_category)
        logging.info(
            "HTTP 请求统计：列表失败 %s 次，详情失败 %s 次",
            stats.list_failures,
            stats.detail_failures,
        )
        return stats

    def _resolve_category_mappings(
        self, target_categories: Optional[List[str]]
    ) -> List[CategoryMapping]:
        extra_cfg = getattr(self._provider, "extra", {}) or {}
        mappings = self._db.fetch_category_mappings(
            self._provider.company_id,
            category_ids=target_categories,
            only_leaf=(target_categories is None),
        )
        if not mappings:
            fallback = self._build_default_mapping(extra_cfg)
            if fallback:
                logging.info(
                    "数据库分类表为空，使用默认分类 %s（API=%s）",
                    fallback.db_category_id,
                    fallback.api_category_id or "-",
                )
                mappings = [fallback]
        if not mappings and not target_categories:
            return []
        if target_categories:
            normalized_targets = {cid.upper(): cid for cid in target_categories}
            requested = set(normalized_targets.keys())
            available = {mapping.db_category_id.upper() for mapping in mappings}
            missing = sorted(requested - available)
            if missing:
                fallback_mappings: List[CategoryMapping] = []
                for cid in missing:
                    original_id = normalized_targets.get(cid, cid)
                    fallback = self._build_default_mapping(extra_cfg, db_category_id=original_id)
                    if fallback:
                        fallback_mappings.append(fallback)
                if fallback_mappings:
                    mappings.extend(fallback_mappings)
                    logging.info(
                        "以下分类未在数据库找到，回退到默认 API 分类 %s：%s",
                        fallback_mappings[0].api_category_id or "-",
                        ", ".join(mapping.db_category_id for mapping in fallback_mappings),
                    )
                fallback_ids = {mapping.db_category_id.upper() for mapping in fallback_mappings}
                unresolved = [cid for cid in missing if cid not in fallback_ids]
                if unresolved:
                    raise ValueError(
                        "数据库中找不到以下 category_id，且未配置默认分类：%s"
                        % ", ".join(normalized_targets.get(cid, cid) for cid in unresolved)
                    )
        return mappings

    def _build_default_mapping(
        self, extra_cfg: Dict[str, object], *, db_category_id: Optional[str] = None
    ) -> Optional[CategoryMapping]:
        db_id_source: Optional[str]
        if db_category_id is None:
            raw_default = extra_cfg.get("default_category_id")
            db_id_source = raw_default if isinstance(raw_default, str) else None
        else:
            db_id_source = db_category_id
        if not db_id_source:
            return None
        api_candidate = extra_cfg.get("default_api_category_id")
        api_id: Optional[str]
        if isinstance(api_candidate, str) and api_candidate:
            normalized_candidate = api_candidate.strip()
            if normalized_candidate.isdigit():
                try:
                    api_id = normalize_category_id(normalized_candidate)
                except ValueError as exc:
                    logging.error(
                        "公司 %s 的 default_api_category_id='%s' 非法：%s",
                        self._provider.company_id,
                        api_candidate,
                        exc,
                    )
                    return None
            else:
                api_id = normalized_candidate
        elif db_category_id is not None:
            logging.warning(
                "公司 %s 未配置 default_api_category_id，无法为缺失的分类 %s 使用默认 API 分类",
                self._provider.company_id,
                db_id_source,
            )
            return None
        else:
            api_id = None
        return CategoryMapping(db_category_id=db_id_source, api_category_id=api_id)

    def _fetch_posts(self, category_id: str, post_limit: Optional[int]) -> List[Dict[str, object]]:
        posts: List[Dict[str, object]] = []
        page = 1
        while True:
            payload = self._provider.build_list_params(category_id, page)
            try:
                response = self._http.fetch_json(
                    self._provider.list_endpoint,
                    payload,
                    headers=self._provider.list_headers(),
                )
            except Exception:
                logging.exception("分类 %s 第%s页列表接口请求失败", category_id or "-", page)
                if self._stats:
                    self._stats.record_list_failure()
                break
            list_result = self._provider.parse_list_response(response, page)
            posts.extend(list_result.posts)
            logging.info(
                "分类 %s 第%s页抓取完成，本页%s条，总计%s/%s",
                category_id,
                page,
                len(list_result.posts),
                len(posts),
                list_result.total_count,
            )
            if post_limit and len(posts) >= post_limit:
                posts = posts[:post_limit]
                break
            if not list_result.has_more:
                break
            page += 1
        return posts

    def _fetch_detail(self, post_id: str) -> Dict[str, object]:
        payload = self._provider.build_detail_params(post_id)
        try:
            response = self._http.fetch_json(
                self._provider.detail_endpoint,
                payload,
                headers=self._provider.detail_headers(),
            )
        except Exception:
            if self._stats:
                self._stats.record_detail_failure()
            raise
        return self._provider.parse_detail_response(response)

    def _build_job_record(self, category_id: str, detail: Dict[str, object]) -> JobRecord:
        crawled_at = datetime.utcnow()
        record = self._provider.build_job_record(category_id, detail, crawled_at=crawled_at)
        record.job_type = self._job_type
        return record

    def _persist_record(self, record: JobRecord) -> None:
        if self._dry_run:
            logging.info("[DRY-RUN] Would upsert job: %s", record.job_url)
            return
        existing = self._db.fetch_job_by_url(record.job_url)
        if not existing:
            record.id = self._db.generate_next_job_id(self._provider.company_id)
            record.created_at = record.crawled_at
            self._db.insert_job(record.as_sql_params())
            logging.info("新增职位 %s", record.id)
            return
        record.id = existing["id"]
        changes = self._compute_changes(existing, record)
        metadata_changes = {
            "crawl_status": record.crawl_status,
            "crawled_at": record.crawled_at,
        }
        for key, value in metadata_changes.items():
            if existing.get(key) != value:
                changes[key] = value
        if changes:
            self._db.update_job(record.id, changes)
            logging.info("更新职位 %s", record.id)
        else:
            logging.debug("职位 %s 未发生变化，跳过更新", record.id)

    def _compute_changes(self, existing: Dict[str, object], record: JobRecord) -> Dict[str, object]:
        new_values = record.as_sql_params()
        comparable_fields = [
            "title",
            "category_id",
            "salary",
            "job_type",
            "education",
            "publish_time",
            "location",
            "description",
            "requirement",
            "bonus",
            "work_experience",
        ]
        changes: Dict[str, object] = {}
        for field in comparable_fields:
            if existing.get(field) != new_values[field]:
                changes[field] = new_values[field]
        if changes:
            changes["crawl_status"] = record.crawl_status
            changes["crawled_at"] = record.crawled_at
        return changes
