# 腾讯招聘爬虫

用于按公司分类抓取腾讯招聘官网岗位信息，支持配置化扩展与增量写库。

## 目录
- [环境要求](#环境要求)
- [安装与配置](#安装与配置)
- [运行方式](#运行方式)
- [规则文件说明](#规则文件说明)
- [常见问题](#常见问题)
- [测试](#测试)

## 环境要求
- Python 3.11+（当前开发环境为 3.13）
- MySQL 5.7+/8.0，用于存储 `company/category/job` 数据
- 可访问腾讯招聘接口的网络环境

## 安装与配置
1. **克隆项目并安装依赖**

```bash
pip install -r requirements.txt
```

2. **复制环境变量模板**

```bash
copy .env.example .env  # Windows
# 或
cp .env.example .env    # macOS/Linux
```

根据自身环境填写 `.env`，字段说明如下：

| 变量名        | 说明                       | 默认值 |
|---------------|----------------------------|--------|
| `DB_HOST`     | MySQL 地址                 | 127.0.0.1 |
| `DB_PORT`     | MySQL 端口                 | 3306 |
| `DB_USER`     | 数据库用户名               | root |
| `DB_PASSWORD` | 数据库密码                 | （空） |
| `DB_NAME`     | 数据库名称                 | job_system |

> 任何敏感信息都放在 `.env`，文件已在 `.gitignore` 中忽略，避免泄漏。

3. **准备规则文件**
   - `rules/company.json` 存放各公司的 API 规则。
   - 若需要兜底分类，务必同时设置 `extra.default_category_id`（数据库 ID）与 `extra.default_api_category_id`（接口 8 位 ID）。

4. **数据库准备**
   - 执行 `sql.sql` 初始化数据结构。
   - 预置 `company`、`category`、`job` 基础数据。
   - 确保 `categoryid` 均为 8 位数字，可使用 `crawler.utils.normalize_category_id` 校验。

## 运行方式

```bash
python main.py --rules rules/company.json [--env-file .env] [--dry-run]
```

运行时交互步骤：
1. 输入公司 ID（如 `C001`）。
2. 选择招聘类型：`0` 社会招聘，`1` 校园招聘。
3. 输入要抓取的分类 ID，逗号分隔或输入 `all` 全部分类。
4. 输入每个分类抓取条数，数字或 `all`。

常用参数：
- `--env-file`: 指定自定义 env 文件。
- `--dry-run`: 仅打印写库数据，不改动数据库。
- `--provider`: 手动指定 provider 名称（默认为规则中的 `provider`）。

### 启动前检查
1. 数据库连通、表结构一致。
2. `.env`/环境变量已配置。
3. `rules/company.json` 中对应 `company_id` 的配置存在并最新。
4. 如需默认分类兜底，`default_category_id` 与 `default_api_category_id` 均已填写。

## 规则文件说明

```json
{
  "company_id": "C001",
  "provider": "config",
  "list_api": { "url": "https://...", "default_params": { ... } },
  "detail_api": { "url": "https://...", "default_params": { ... } },
  "throttle": { "min_seconds": 0.5, "max_seconds": 1.0, "max_retries": 3, "retry_backoff": 2.0, "timeout": 15 },
  "extra": {
    "list": { "posts_path": "Data.Posts", ... },
    "detail": { "data_path": "Data", ... },
    "field_map": { "title": "RecruitPostName", ... },
    "default_values": { "salary": "面议" },
    "default_category_id": "CATDEFAULT",
    "default_api_category_id": "40001001"
  }
}
```

- `list_api/detail_api`: 接口地址和基础参数。
- `throttle`: 每次请求的最小/最大延迟、重试次数、退避系数、超时。
- `extra.list/detail`: 描述 JSON 结构，决定如何解析列表与详情。
- `field_map`: 详情 JSON 字段与 `JobRecord` 字段映射。
- `default_values`: 字段缺失时的回退值。
- `default_category_id`: 当数据库没有叶子分类时仍需写库的默认 DB 分类 ID。
- `default_api_category_id`: 与上项配对的 8 位接口分类 ID，命令行输入不存在分类时也会使用它回退。
- `headers/list_headers/detail_headers`: 可选 HTTP 头，支持 `${ENV_VAR}`。

## 日志与增量策略
- HTTP 客户端自带节流及重试，若响应不是合法 JSON，会记录状态码与片段并重试。
- `JobCrawler` 会统计每个分类成功/失败数量，并在 `dry-run` 模式下只打印 SQL。
- 以 `job_url` 判重，新增自动生成 `C001Jxxxxx`，已有数据仅更新变更字段与 `crawl_status`、`crawled_at`。

## 常见问题
- **ModuleNotFoundError: crawler**：请确认从仓库根目录运行 `pytest` 或 `python main.py`，测试已在 `tests/conftest.py` 中处理路径。
- **分类缺失**：若数据库没有目标 `category_id`，确保 `rules` 中提供默认分类对；否则程序会抛出异常提醒配置。
- **JSON 解析失败**：日志会显示状态码和响应片段，可检查是否被 WAF/代理拦截或需要额外头信息。

## 测试

```bash
pytest
```

测试涵盖时间解析、分类兜底、HTTP JSON 失败重试等关键逻辑，建议在修改规则或核心代码后运行。

