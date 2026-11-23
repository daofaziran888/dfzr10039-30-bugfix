"""采集链路端到端测试：站点→作业→抽取→清洗→存储，使用真实数据样本。"""
from __future__ import annotations

from app.integration import (
    run_crawl_pipeline, recover_failed_batch, handle_duplicate_batch,
    handle_interrupted_pipeline, _simulate_extract, _apply_clean_rules,
)
from app.models import (
    CleanRule, CleanType, ExtractRule, CrawlJobSpec, SiteConfig, SiteType,
    StorageBatch, CleanedContent, RawExtractedContent,
)
from app.store import store


# ---------------------------------------------------------------------------
# 测试数据
# ---------------------------------------------------------------------------

VALID_SITE = {
    "id": 1, "status": "已验证", "pending": False, "abnormal": False,
    "站点编号": "CRAW-T001", "站点域名": "news.example.com",
    "站点类型": "新闻门户", "Robots协议": "allow",
    "站点语言": "zh-CN", "地域归属": "CN",
    "接入方式": "direct", "注册时间": "2026-11-01",
}

UNVERIFIED_SITE = {
    "id": 2, "status": "待验证", "pending": True, "abnormal": False,
    "站点编号": "CRAW-T002", "站点域名": "unverified.example.com",
    "站点类型": "新闻门户", "Robots协议": "disallow",
    "站点语言": "zh-CN", "地域归属": "CN",
    "接入方式": "direct", "注册时间": "2026-11-01",
}

VALID_JOB = {
    "id": 1, "status": "待执行", "pending": True, "abnormal": False,
    "作业编号": "CRAW-JOB-T001", "目标站点": "news.example.com",
    "入口URL": "https://news.example.com/article/1",
    "最大深度": "3", "并发线程": "4",
    "调度策略": "interval", "完成页数": "0", "失败页数": "0",
}

VALID_EXTRACT_RULE = {
    "id": 1, "status": "生效中", "pending": False, "abnormal": False,
    "规则编号": "CONT-T001", "目标站点": "news.example.com",
    "XPath选择器": "//article/div[@class='content']",
    "字段映射": "title,body,publish_date",
    "数据类型": "string", "清洗正则": "",
    "抽取顺序": "1", "规则状态": "生效中",
}

VALID_CLEAN_RULE = {
    "id": 1, "status": "生效中", "pending": False, "abnormal": False,
    "规则编号": "FIEL-T001", "目标字段": "title",
    "清洗类型": "去空格", "处理表达式": "",
    "空值策略": "skip", "校验范围": "",
    "清洗顺序": "1", "规则状态": "生效中",
}


# ---------------------------------------------------------------------------
# 测试：端到端采集链路
# ---------------------------------------------------------------------------

def test_full_crawl_pipeline_success() -> None:
    """正常采集链路：已验证站点 → 作业创建 → 抽取 → 清洗 → 存储。"""
    batch = run_crawl_pipeline(
        site_row=VALID_SITE,
        job_row=VALID_JOB,
        extract_rows=[VALID_EXTRACT_RULE],
        clean_rows=[VALID_CLEAN_RULE],
    )

    assert batch is not None, "正常采集应返回存储批次"
    assert batch.status == "校验通过", f"批次状态应为校验通过，实际: {batch.status}"
    assert batch.row_count >= 1, f"应有至少1行数据，实际: {batch.row_count}"
    assert batch.checksum, "应生成校验摘要"
    assert batch.source_job_id == "CRAW-JOB-T001", "批次应关联采集作业"


def test_pipeline_rejects_unverified_site() -> None:
    """未验证站点应被拒绝采集。"""
    batch = run_crawl_pipeline(
        site_row=UNVERIFIED_SITE,
        job_row=VALID_JOB,
        extract_rows=[VALID_EXTRACT_RULE],
        clean_rows=[VALID_CLEAN_RULE],
    )

    assert batch is None, "未验证站点不应返回批次"


def test_pipeline_with_multiple_urls() -> None:
    """多 URL 采集链路：验证多条记录被正确抽取和清洗。"""
    job = dict(VALID_JOB)
    job["入口URL"] = (
        "https://news.example.com/article/1,"
        "https://news.example.com/article/2,"
        "https://news.example.com/article/3"
    )

    batch = run_crawl_pipeline(
        site_row=VALID_SITE,
        job_row=job,
        extract_rows=[VALID_EXTRACT_RULE],
        clean_rows=[VALID_CLEAN_RULE],
    )

    assert batch is not None
    assert batch.row_count == 3, f"应有3条记录，实际: {batch.row_count}"


def test_pipeline_handles_empty_url() -> None:
    """空 URL 采集应正确处理并产生错误记录。"""
    job = dict(VALID_JOB)
    job["入口URL"] = "https://empty.example.com/page"

    batch = run_crawl_pipeline(
        site_row=VALID_SITE,
        job_row=job,
        extract_rows=[VALID_EXTRACT_RULE],
        clean_rows=[VALID_CLEAN_RULE],
    )

    assert batch is not None
    assert batch.status == "校验失败", f"空页面应导致校验失败，实际: {batch.status}"


def test_pipeline_handles_error_url() -> None:
    """错误 URL 采集应正确处理 404/错误场景。"""
    job = dict(VALID_JOB)
    job["入口URL"] = "https://news.example.com/error/404"

    batch = run_crawl_pipeline(
        site_row=VALID_SITE,
        job_row=job,
        extract_rows=[VALID_EXTRACT_RULE],
        clean_rows=[VALID_CLEAN_RULE],
    )

    assert batch is not None
    # 错误 URL 不产生有效记录
    assert batch.row_count == 0 or batch.status == "校验失败", \
        f"错误URL应产生空批次或校验失败，actual row_count={batch.row_count} status={batch.status}"


# ---------------------------------------------------------------------------
# 测试：抽取模拟逻辑
# ---------------------------------------------------------------------------

def test_extract_normal_url() -> None:
    """正常 URL 抽取产生字段数据。"""
    rule = ExtractRule.from_seed_row(VALID_EXTRACT_RULE)
    rule.field_mapping = {"title": "//h1", "body": "//div[@class='content']"}

    raw = _simulate_extract("https://news.example.com/article/1", rule)

    assert not raw.has_errors(), f"正常URL不应有错误，实际: {raw.errors}"
    assert raw.raw_fields, "应抽取到字段数据"
    assert "title" in raw.raw_fields, "应包含 title 字段"


def test_extract_error_url() -> None:
    """包含 error 的 URL 产生错误。"""
    rule = ExtractRule.from_seed_row(VALID_EXTRACT_RULE)
    raw = _simulate_extract("https://example.com/error/page", rule)

    assert raw.has_errors(), "错误URL应标记错误"
    assert "页面不可达" in " ".join(raw.errors)


def test_extract_empty_url() -> None:
    """包含 empty 的 URL 返回空字段。"""
    rule = ExtractRule.from_seed_row(VALID_EXTRACT_RULE)
    raw = _simulate_extract("https://empty.example.com/page", rule)

    assert not raw.raw_fields, "空页面应无字段"


# ---------------------------------------------------------------------------
# 测试：清洗逻辑
# ---------------------------------------------------------------------------

def test_clean_trim_rule() -> None:
    """去空格清洗规则正确执行。"""
    raw = RawExtractedContent(
        source_url="https://test.example.com",
        extract_rule_id="CONT-T001",
        raw_fields={"title": "   Hello World   "},
    )
    rules = [CleanRule(
        rule_id="FIEL-T001", target_field="title",
        clean_type=CleanType.TRIM, null_strategy="skip",
    )]

    result = _apply_clean_rules(raw, rules)

    assert result.fields["title"] == "Hello World", f"去空格后应为'Hello World'，实际: {result.fields['title']}"


def test_clean_normalize_rule() -> None:
    """标准化清洗规则正确执行。"""
    raw = RawExtractedContent(
        source_url="https://test.example.com",
        extract_rule_id="CONT-T001",
        raw_fields={"category": "Data Engineering"},
    )
    rules = [CleanRule(
        rule_id="FIEL-T002", target_field="category",
        clean_type=CleanType.NORMALIZE, null_strategy="skip",
    )]

    result = _apply_clean_rules(raw, rules)

    assert result.fields["category"] == "data_engineering", \
        f"标准化后应为'data_engineering'，实际: {result.fields['category']}"


def test_clean_default_fill() -> None:
    """默认填充策略处理空值字段。"""
    raw = RawExtractedContent(
        source_url="https://test.example.com",
        extract_rule_id="CONT-T001",
        raw_fields={"author": ""},
    )
    rules = [CleanRule(
        rule_id="FIEL-T003", target_field="author",
        clean_type=CleanType.DEFAULT_FILL, null_strategy="fill_default",
    )]

    result = _apply_clean_rules(raw, rules)

    assert result.fields.get("author") == "N/A", f"空值应填充为N/A，实际: {result.fields.get('author')}"


def test_clean_multiple_rules() -> None:
    """多条清洗规则按顺序执行。"""
    raw = RawExtractedContent(
        source_url="https://test.example.com",
        extract_rule_id="CONT-T001",
        raw_fields={"title": "  TEST Title  ", "author": ""},
    )
    rules = [
        CleanRule(rule_id="R1", target_field="title", clean_type=CleanType.TRIM, null_strategy="skip"),
        CleanRule(rule_id="R2", target_field="title", clean_type=CleanType.NORMALIZE, null_strategy="skip"),
        CleanRule(rule_id="R3", target_field="author", clean_type=CleanType.DEFAULT_FILL, null_strategy="fill_default"),
    ]

    result = _apply_clean_rules(raw, rules)

    assert result.fields["title"] == "test_title", f"清洗后应为'test_title'，实际: {result.fields['title']}"
    assert result.fields["author"] == "N/A", f"空值应填充为N/A，实际: {result.fields['author']}"


# ---------------------------------------------------------------------------
# 测试：失败处理和恢复
# ---------------------------------------------------------------------------

def test_recover_failed_batch() -> None:
    """恢复失败批次：无实际响应时禁止伪造 recovered 占位值，批次保持校验失败。"""
    # record1: 有效字段
    # record2: 空字段导致 is_valid() 返回 False，且批次未留存任何实际响应
    records = [
        CleanedContent(source_url="url1", extract_rule_id="R1",
                       fields={"body": "content"}, clean_errors=[]),
        CleanedContent(source_url="url2", extract_rule_id="R1",
                       fields={"title": ""}, clean_errors=["empty"]),
    ]
    batch = StorageBatch(
        batch_id="BATCH-RECOVER-001", source_job_id="JOB-001",
        row_count=2, status="校验失败", records=records,
        checksum="old_checksum",
        storage_path="/tmp/test-recover/",
    )

    recovered = recover_failed_batch(batch)

    assert recovered is not None, "无响应时应返回带阻断原因的批次而非静默成功"
    assert recovered.status == "校验失败", \
        f"无实际响应不允许占位通过，实际: {recovered.status}"
    assert "url2" in recovered.recovery_blockers, "未通过原因必须逐行保留"
    # 不得再出现伪造的 recovered: 标题
    assert all(
        not str(value).startswith("recovered:")
        for record in recovered.records for value in record.fields.values()
    ), "恢复结果中不允许出现 recovered: 占位值"
    assert recovered.checksum == "old_checksum", "未通过重放不应改写校验摘要"


def test_recover_already_valid_batch() -> None:
    """已通过校验的批次不需要恢复。"""
    batch = StorageBatch(
        batch_id="BATCH-VALID-001", source_job_id="JOB-001",
        row_count=1, status="校验通过",
        records=[CleanedContent(source_url="url1", extract_rule_id="R1", fields={"title": "ok"})],
    )

    recovered = recover_failed_batch(batch)

    assert recovered is None, "已通过批次不需要恢复"


def test_handle_duplicate_batch() -> None:
    """重复批次：追加时间戳后缀避免 ID 冲突。"""
    batch = StorageBatch(
        batch_id="BATC-0001", source_job_id="JOB-001",
        row_count=10, storage_path="/data/batches/JOB-001/",
        records=[],
    )

    # 已有同批次ID
    store.rows("batch_store").append({
        "id": 99, "批次编号": "BATC-0001", "来源作业": "old",
        "数据行数": 5, "存储路径": "/old/", "压缩格式": "gzip",
        "Schema版本": "v1", "校验摘要": "abc", "入库时间": "",
        "status": "已落盘", "pending": False, "abnormal": False,
    })

    handled = handle_duplicate_batch(batch)

    assert handled is not None
    assert handled.batch_id != "BATC-0001", "重复批次应重命名"
    assert "-retry" in handled.storage_path, "重试批次存储路径应标记 retry"


def test_handle_interrupted_pipeline() -> None:
    """中断管线恢复：失败作业重新进入执行状态。"""
    job = CrawlJobSpec(
        job_id="JOB-INT-001", site=SiteConfig(
            site_id="S1", domain="test.com", site_type=SiteType.NEWS,
            robots_policy="allow", language="zh-CN", region="CN",
            access_method="direct", registered_at="2026-01-01",
        ),
        entry_urls=["https://test.com"], completed_pages=5,
        failed_pages=3, status="部分失败",
    )

    recovered = handle_interrupted_pipeline(job)

    assert recovered is not None
    assert recovered.failed_pages < 3, "失败页面应减少"
    assert recovered.status == "采集执行", f"状态应为采集执行，实际: {recovered.status}"


# ---------------------------------------------------------------------------
# 测试：数据模型
# ---------------------------------------------------------------------------

def test_site_config_from_row() -> None:
    """SiteConfig 从种子行正确创建。"""
    site = SiteConfig.from_seed_row(VALID_SITE)

    assert site.site_id == "CRAW-T001"
    assert site.domain == "news.example.com"
    assert site.site_type == SiteType.NEWS
    assert site.is_crawlable(), "已验证且 Robots 允许的站点应可采集"


def test_crawl_job_spec_from_row() -> None:
    """CrawlJobSpec 从种子行正确创建。"""
    job = CrawlJobSpec.from_seed_row(VALID_JOB)

    assert job.job_id == "CRAW-JOB-T001"
    assert len(job.entry_urls) == 1
    assert job.max_depth == 3


def test_url_fingerprint_computation() -> None:
    """URL 指纹计算：相同 URL 应产生相同哈希。"""
    fp1 = UrlFingerprint.compute("https://example.com/page?a=1&b=2")
    fp2 = UrlFingerprint.compute("https://example.com/page?b=2&a=1")

    assert fp1.hash_value == fp2.hash_value, "参数顺序不同应产生相同指纹"


def test_proxy_node_availability() -> None:
    """代理节点可用性判断。"""
    available = ProxyNode(
        node_id="P1", address="1.2.3.4:8080",
        proxy_type=ProxyType.HTTP, region_tag="CN", operator="telecom",
        latency_ms=500, success_rate=0.95, status="可用",
    )

    assert available.is_available(), "正常代理应可用"

    unavailable = ProxyNode(
        node_id="P2", address="5.6.7.8:8080",
        proxy_type=ProxyType.HTTP, region_tag="CN", operator="telecom",
        latency_ms=5000, success_rate=0.5, status="延迟告警",
    )

    assert not unavailable.is_available(), "高延迟低成功率代理应不可用"


def test_storage_batch_checksum() -> None:
    """批次校验和：相同记录数据产生相同校验和。"""
    records = [
        CleanedContent(source_url="u1", extract_rule_id="r1", fields={"a": "1", "b": "2"}),
        CleanedContent(source_url="u2", extract_rule_id="r1", fields={"a": "1", "b": "3"}),
    ]

    batch1 = StorageBatch(batch_id="B1", source_job_id="J1", row_count=2, records=records)
    batch2 = StorageBatch(batch_id="B2", source_job_id="J2", row_count=2, records=records)

    assert batch1.compute_checksum() == batch2.compute_checksum(), \
        "相同记录数据应产生相同校验和"


# ---------------------------------------------------------------------------
# 测试：解析日志持久化
# ---------------------------------------------------------------------------

def test_parse_log_in_pipeline() -> None:
    """采集链路中解析日志被正确记录。"""
    before_count = len(store.rows("parse_log"))

    job = dict(VALID_JOB)
    job["入口URL"] = (
        "https://news.example.com/article/1,"
        "https://news.example.com/error/404,"
        "https://empty.example.com/page"
    )

    batch = run_crawl_pipeline(
        site_row=VALID_SITE,
        job_row=job,
        extract_rows=[VALID_EXTRACT_RULE],
        clean_rows=[VALID_CLEAN_RULE],
    )

    after_count = len(store.rows("parse_log"))
    assert after_count >= before_count + 3, \
        f"应有至少3条解析日志，before={before_count} after={after_count}"


# ---------------------------------------------------------------------------
# 测试：S11 容量辅助导入（使模块可见 soma models）
# ---------------------------------------------------------------------------

from app.models import (
    CleanRule, CleanType, CrawlJobSpec, ExtractRule, ProxyNode, ProxyType,
    RawExtractedContent, SiteConfig, SiteType, StorageBatch, UrlFingerprint,
)