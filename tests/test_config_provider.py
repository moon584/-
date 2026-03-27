from datetime import UTC, datetime

from crawler.providers.config_provider import ConfigDrivenProvider
from crawler.rules import APIEndpoint, CompanyRule, ThrottleRule


def _make_rule(extra: dict) -> CompanyRule:
    return CompanyRule(
        company_id="TEST",
        company_name="Test",
        provider="config",
        list_api=APIEndpoint(url="https://example.com/list", default_params={}),
        detail_api=APIEndpoint(url="https://example.com/detail", default_params={}),
        throttle=ThrottleRule(min_seconds=0.1, max_seconds=0.2, max_retries=1, retry_backoff=1.0, timeout=5),
        extra=extra,
    )


def test_url_template_overrides_job_url() -> None:
    extra = {
        "field_map": {
            "job_url": "jobUnionId",
            "title": "name",
            "description": "jobDuty",
        },
        "default_values": {
            "salary": "面议",
        },
        "url_templates": {
            "job_url": "https://jobs.example.com/{jobUnionId}",
        },
    }
    provider = ConfigDrivenProvider(_make_rule(extra))
    detail = {
        "jobUnionId": "123456",
        "name": "测试工程师",
        "jobDuty": "负责测试",
    }

    record = provider.build_job_record("CAT001", detail, crawled_at=datetime.now(UTC))

    assert record.job_url == "https://jobs.example.com/123456"


def test_resolve_path_supports_list_index() -> None:
    extra = {
        "field_map": {
            "job_url": "jobUnionId",
            "title": "name",
            "description": "jobDuty",
        },
        "default_values": {
            "salary": "面议",
        },
    }
    provider = ConfigDrivenProvider(_make_rule(extra))
    payload = {
        "cityList": [
            {"name": "上海"},
            {"name": "北京"},
        ]
    }

    assert provider._resolve_path(payload, "cityList.0.name") == "上海"
    assert provider._resolve_path(payload, "cityList.1.name") == "北京"
    assert provider._resolve_path(payload, "cityList.5.name") is None


def test_category_rule_matches_combined_context() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
        "category_rules": [
            {
                "category_id": "C002TEST",
                "match": {
                    "jobFamily": "技术类",
                    "jobFamilyGroup": "测试",
                },
            }
        ],
        "auto_category_mode": True,
    }
    provider = ConfigDrivenProvider(_make_rule(extra))
    post = {"jobFamily": "技术类", "jobFamilyGroup": "测试"}

    assert provider.resolve_category_id(post, detail={}) == "C002TEST"


def test_category_rule_returns_none_when_not_matched() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
        "category_rules": [
            {
                "category_id": "C002TEST",
                "match": {"jobFamilyGroup": "测试"},
            }
        ],
        "auto_category_mode": True,
    }
    provider = ConfigDrivenProvider(_make_rule(extra))
    post = {"jobFamily": "技术类", "jobFamilyGroup": "研发"}

    assert provider.resolve_category_id(post, detail={}) is None


def test_string_field_combines_city_list() -> None:
    extra = {
        "field_map": {
            "job_url": "jobUnionId",
            "title": "name",
            "location": "cityList",
        },
        "default_values": {
            "salary": "面议",
        },
    }
    provider = ConfigDrivenProvider(_make_rule(extra))
    detail = {
        "jobUnionId": "123",
        "name": "岗位",
        "cityList": [
            {"name": "上海"},
            {"name": "北京"},
        ],
    }

    record = provider.build_job_record("CAT001", detail, crawled_at=datetime.now(UTC))

    assert record.location == "上海 / 北京"


def test_build_list_params_prunes_empty_strings() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
    }
    rule = _make_rule(extra)
    rule.list_api.default_params.update(
        {
            "jobFamily": "",
            "keyWord": "   ",
            "pageSize": 20,
        }
    )
    provider = ConfigDrivenProvider(rule)

    params = provider.build_list_params(category_id="", page=2)

    assert "jobFamily" not in params
    assert "keyWord" not in params
    assert params["pageIndex"] == 2
    assert params["pageSize"] == 20


def test_supports_auto_category_without_rules() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
        "auto_category_mode": True,
    }
    provider = ConfigDrivenProvider(_make_rule(extra))

    assert provider.supports_auto_category() is True


def test_build_list_params_supports_nested_page_path() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
        "list": {
            "page_param": "page.pageNo",
            "size_param": "page.pageSize",
            "page_size": 10,
        },
    }
    rule = _make_rule(extra)
    rule.list_api.default_params = {
        "page": {
            "pageNo": 1,
            "pageSize": 10,
        }
    }
    provider = ConfigDrivenProvider(rule)

    params = provider.build_list_params(category_id="", page=3)

    assert params["page"]["pageNo"] == 3
    assert params["page"]["pageSize"] == 10


def test_build_list_params_keeps_configured_empty_string_fields() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
        "preserve_empty_string_fields": ["keywords"],
    }
    rule = _make_rule(extra)
    rule.list_api.default_params = {
        "keywords": "",
        "jobFamily": "",
        "pageSize": 20,
    }
    provider = ConfigDrivenProvider(rule)

    params = provider.build_list_params(category_id="", page=1)

    assert params["keywords"] == ""
    assert "jobFamily" not in params


def test_parse_list_response_accepts_success_values() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
        "list": {
            "code_field": "status",
            "data_path": "data",
            "posts_path": "list",
            "count_path": "page.totalCount",
            "page_size": 10,
            "success_value": 0,
            "success_values": [0, 1],
        },
    }
    provider = ConfigDrivenProvider(_make_rule(extra))
    payload = {
        "status": 1,
        "data": {
            "list": [{"jobUnionId": "1"}],
            "page": {"totalCount": 1},
        },
    }

    result = provider.parse_list_response(payload, page=1)

    assert len(result.posts) == 1
    assert result.total_count == 1
    assert result.has_more is False


def test_parse_list_response_rejects_unexpected_status_by_default() -> None:
    extra = {
        "field_map": {"job_url": "jobUnionId"},
        "list": {
            "code_field": "status",
            "data_path": "data",
            "posts_path": "list",
            "count_path": "page.totalCount",
            "page_size": 10,
            "success_value": 0,
        },
    }
    provider = ConfigDrivenProvider(_make_rule(extra))
    payload = {
        "status": 1,
        "data": {
            "list": [],
            "page": {"totalCount": 0},
        },
    }

    try:
        provider.parse_list_response(payload, page=1)
        raise AssertionError("expected parse_list_response to raise RuntimeError")
    except RuntimeError as exc:
        assert "Code=1" in str(exc)


