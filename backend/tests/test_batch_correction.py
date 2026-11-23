"""失败批更正重放测试（值班员恢复缺标题采集记录场景）。

场景序列：
1. 无实际响应时先重跑一次，确认占位结果不会通过，未通过原因被保留；
2. 同时提交原批响应与补采响应，标题冲突按优先级裁决，入库标题取自原批证据；
3. 从批次存储详情核对真实标题、来源快照与回查证据；
4. 同一次更正再发一遍，确认批次仍只有一个有效恢复版本且同批另一篇文章未被替换。
"""
from __future__ import annotations

import copy

import pytest

from app.models import (
    CapturedResponse, CleanRule, CleanType, CleanedContent, ExtractRule,
    StorageBatch,
)
from app.services.batch_recovery import (
    register_captured_response, replay_failed_batch,
)
from app.store import store


@pytest.fixture(autouse=True)
def _isolate_correction_batch_rows():
    """全局内存 store 是单例，按批次编号清理本文件产生的存储行，隔离用例。"""
    def purge() -> None:
        table = store.rows("batch_store")
        table[:] = [
            row for row in table if str(row.get("批次编号")) != BATCH_ID
        ]

    purge()
    yield
    purge()


# ---------------------------------------------------------------------------
# 测试夹具
# ---------------------------------------------------------------------------

BAD_URL = "https://news.example.com/article/bad-title"
GOOD_URL = "https://news.example.com/article/good-article"
BATCH_ID = "BATCH-CORRECTION-T001"

ORIGINAL_TITLE = "原批采集的真实标题"
SUPPLEMENT_TITLE = "补采取得的不同新标题"


def _make_rule() -> ExtractRule:
    return ExtractRule(
        rule_id="CONT-CORR-T001",
        target_site="news.example.com",
        xpath_selector="//article//title|//body",
        field_mapping={"raw": "title,body,publish_date"},
        status="生效中",
    )


def _make_clean_rules() -> list[CleanRule]:
    return [
        CleanRule(rule_id="FIEL-CORR-T1", target_field="title",
                  clean_type=CleanType.TRIM, null_strategy="skip", status="生效中"),
        CleanRule(rule_id="FIEL-CORR-T2", target_field="body",
                  clean_type=CleanType.TRIM, null_strategy="skip", status="生效中"),
    ]


def _make_batch() -> StorageBatch:
    """失败行 BAD_URL 缺标题；同批 GOOD_URL 已通过，恢复中不得被替换。"""
    bad = CleanedContent(
        source_url=BAD_URL, extract_rule_id="CONT-CORR-T001",
        fields={"title": "", "body": "原批正文"}, clean_errors=["标题缺失"],
    )
    good = CleanedContent(
        source_url=GOOD_URL, extract_rule_id="CONT-CORR-T001",
        fields={"title": "同批另一篇通过校验的文章", "body": "保持不变的正文"},
    )
    return StorageBatch(
        batch_id=BATCH_ID, source_job_id="CRAW-CORR-JOB",
        row_count=2, status="校验失败", records=[bad, good],
        checksum="checksum-before-correction",
        storage_path="/data/batches/CRAW-CORR-JOB/",
    )


def _original_response() -> CapturedResponse:
    return CapturedResponse(
        snapshot_id="SNAP-ORIG-001",
        source_url=BAD_URL,
        origin="original_capture",
        http_status=200,
        body=(
            "<html><head><title>  原批采集的真实标题  </title></head>"
            "<body><h1>原批采集的真实标题</h1><p>原批正文</p>"
            "发布时间:2026-11-20</body></html>"
        ),
        captured_at="2026-11-20T08:00:00+00:00",
    )


def _supplemental_response() -> CapturedResponse:
    return CapturedResponse(
        snapshot_id="SNAP-SUPP-001",
        source_url=BAD_URL,
        origin="supplemental",
        http_status=200,
        body=(
            "<html><head><title>补采取得的不同新标题</title></head>"
            "<body><h1>补采取得的不同新标题</h1><p>补采正文</p></body></html>"
        ),
        captured_at="2026-11-23T02:00:00+00:00",
    )


def _no_response_snapshot() -> CapturedResponse:
    return CapturedResponse(
        snapshot_id="SNAP-DEAD-001",
        source_url=BAD_URL,
        origin="supplemental",
        http_status=504,
        body="",
        available=False,
        failure_reason="网关超时，未取到响应正文",
        captured_at="2026-11-23T01:00:00+00:00",
    )


def _store_row_id(batch_id: str) -> int | None:
    for row in store.rows("batch_store"):
        if str(row.get("批次编号")) == batch_id:
            return int(row["id"])
    return None


def _find_store_row(batch_id: str) -> dict:
    return next(
        row for row in store.rows("batch_store")
        if str(row.get("批次编号")) == batch_id
    )


# ---------------------------------------------------------------------------
# 阶段 1：无响应重跑，占位结果不通过
# ---------------------------------------------------------------------------

def test_replay_without_response_never_passes() -> None:
    batch = _make_batch()
    rule = _make_rule()
    clean_rules = _make_clean_rules()

    # 只登记一个无响应的补采快照（模拟补采也失败）
    register_captured_response(batch, _no_response_snapshot())

    receipt = replay_failed_batch(batch, rule, clean_rules)

    assert receipt is not None, "无响应也应给出失败批更正回执"
    assert receipt.overall_passed is False, "无响应占位结果不允许通过"
    line = next(line for line in receipt.lines if line.source_url == BAD_URL)
    assert line.passed is False
    assert line.chosen_snapshot_id is None, "无响应不得选出任何来源快照"
    assert "未通过" in line.failure_reason or "无可用实际响应" in line.failure_reason
    assert "网关超时" in line.failure_reason, "无响应的未通过原因必须保留"

    # 批次保持校验失败，标题仍然为空，绝不出现 recovered: 占位
    assert batch.status == "校验失败"
    bad_record = next(r for r in batch.records if r.source_url == BAD_URL)
    assert bad_record.fields.get("title", "") == ""
    assert all(
        not str(value).startswith("recovered:")
        for record in batch.records for value in record.fields.values()
    )
    # 失败的占位重跑不计入有效恢复版本
    assert batch.active_recovery_versions == 0


def test_replay_without_any_snapshot_records_blocker() -> None:
    """批次未留存任何响应时，失败原因同样保留且不通过。"""
    batch = _make_batch()
    receipt = replay_failed_batch(batch, _make_rule(), _make_clean_rules())

    assert receipt is not None
    assert receipt.overall_passed is False
    line = receipt.lines[0]
    assert line.passed is False
    assert line.failure_reason, "无响应要保留未通过的原因"
    assert batch.status == "校验失败"


# ---------------------------------------------------------------------------
# 阶段 2：旧响应与补采响应并存，标题冲突按优先级裁决
# ---------------------------------------------------------------------------

def test_replay_with_conflicting_responses_prefers_original_capture() -> None:
    batch = _make_batch()
    rule = _make_rule()
    clean_rules = _make_clean_rules()

    # 值班员找回最初采集的响应，同时当前页面补采拿到不同新标题
    register_captured_response(batch, _original_response())
    register_captured_response(batch, _supplemental_response())

    good_before = copy.deepcopy(
        next(r for r in batch.records if r.source_url == GOOD_URL).fields
    )
    receipt = replay_failed_batch(batch, rule, clean_rules)

    assert receipt.overall_passed is True, "有实际响应且重放有效时应通过"
    line = next(line for line in receipt.lines if line.source_url == BAD_URL)
    assert line.passed is True

    # 标题冲突：入库标题必须是原批证据标题，而非补采的新标题
    assert line.fields["title"] == ORIGINAL_TITLE, \
        f"优先级应让原批证据标题入库，实际: {line.fields['title']}"
    assert line.fields["title"] != SUPPLEMENT_TITLE
    assert line.chosen_origin == "original_capture"
    assert line.chosen_origin_label == "原批证据"

    # 摘要与实际网页对得上：正文同样取自原批响应，且没有任何 recovered: 伪造值
    bad_record = next(r for r in batch.records if r.source_url == BAD_URL)
    assert bad_record.fields["title"] == ORIGINAL_TITLE
    assert "原批正文" in bad_record.fields["body"]
    assert all(
        not str(value).startswith("recovered:")
        for record in batch.records for value in record.fields.values()
    )

    # 冲突过程留痕：两个候选标题都在回执里，裁决说明可回查
    assert len(receipt.title_conflicts) == 1
    conflict = receipt.title_conflicts[0]
    candidate_titles = {item["title"] for item in conflict["candidates"]}
    assert {ORIGINAL_TITLE, SUPPLEMENT_TITLE} <= candidate_titles
    assert conflict["chosen_origin"] == "original_capture"
    assert "优先级" in conflict["resolution"]

    # 同批另一篇通过校验的文章没有被替换
    good_after = next(r for r in batch.records if r.source_url == GOOD_URL)
    assert good_after.fields == good_before
    assert good_after.fields["title"] == "同批另一篇通过校验的文章"

    # 字段结果可回查实际响应与规则
    evidence = line.evidence
    assert evidence["extract_rule_id"] == "CONT-CORR-T001"
    assert "FIEL-CORR-T1" in evidence["clean_rule_ids"]
    assert evidence["http_status"] == 200
    assert any(item["snapshot_id"] == "SNAP-SUPP-001" for item in evidence["candidate_titles"])
    assert bad_record.provenance["source_snapshot_id"] == "SNAP-ORIG-001"
    assert bad_record.provenance["source_origin_label"] == "原批证据"


def test_priority_flip_changes_chosen_title_and_is_auditable() -> None:
    """显式调整优先级会改变入库标题，但裁决依据仍在回执中可回查。"""
    batch = _make_batch()
    register_captured_response(batch, _original_response())
    register_captured_response(batch, _supplemental_response())

    receipt = replay_failed_batch(
        batch, _make_rule(), _make_clean_rules(),
        priority=["supplemental", "original_capture"],
    )

    line = next(line for line in receipt.lines if line.source_url == BAD_URL)
    assert line.fields["title"] == SUPPLEMENT_TITLE
    assert line.chosen_origin == "supplemental"
    assert receipt.priority == ["supplemental", "original_capture"]
    assert receipt.title_conflicts[0]["chosen_origin"] == "supplemental"


# ---------------------------------------------------------------------------
# 阶段 3：批次存储详情核对（清单与明细同取一份回执）
# ---------------------------------------------------------------------------

def test_batch_store_detail_syncs_real_title_and_source_snapshots() -> None:
    batch = _make_batch()
    register_captured_response(batch, _original_response())
    register_captured_response(batch, _supplemental_response())
    replay_failed_batch(batch, _make_rule(), _make_clean_rules())

    row = _find_store_row(BATCH_ID)

    # 状态与摘要已同步到批次存储清单
    assert row["status"] == "校验通过"
    assert row["校验摘要"] == batch.checksum[:16]
    assert row["数据行数"] == 2

    receipt = row["失败批更正回执"]
    assert receipt is not None
    # 存储明细与重放结果取自同一份回执
    assert row["重放明细"] == receipt["lines"]
    assert row["标题冲突裁决"] == receipt["title_conflicts"]

    line = row["重放明细"][0]
    assert line["fields"]["title"] == ORIGINAL_TITLE
    assert line["chosen_origin_label"] == "原批证据"

    # 来源快照可回查：原批证据与后续补采分别标注
    snapshots = row["来源快照"][BAD_URL]
    origins = {snap["origin"]: snap for snap in snapshots}
    assert set(origins) == {"original_capture", "supplemental"}
    assert origins["original_capture"]["origin_label"] == "原批证据"
    assert origins["supplemental"]["origin_label"] == "后续补采"
    assert ORIGINAL_TITLE in origins["original_capture"]["body"]
    assert SUPPLEMENT_TITLE in origins["supplemental"]["body"]


def test_failed_replay_syncs_blocker_reason_to_store() -> None:
    """失败的重放也同步到存储清单，未通过原因可从批次存储详情回查。"""
    batch = _make_batch()
    register_captured_response(batch, _no_response_snapshot())
    replay_failed_batch(batch, _make_rule(), _make_clean_rules())

    row = _find_store_row(BATCH_ID)
    assert row["status"] == "校验失败"
    assert row["abnormal"] is True
    assert row["失败批更正回执"]["overall_passed"] is False
    assert "网关超时" in row["重放明细"][0]["failure_reason"]
    assert row["有效恢复版本数"] == 0


# ---------------------------------------------------------------------------
# 阶段 4：同一次更正重发的幂等性与兄弟行保护
# ---------------------------------------------------------------------------

def test_same_correction_resubmitted_stays_single_recovery_version() -> None:
    batch = _make_batch()
    register_captured_response(batch, _original_response())
    register_captured_response(batch, _supplemental_response())

    first = replay_failed_batch(batch, _make_rule(), _make_clean_rules())
    assert batch.active_recovery_versions == 1
    first_receipt_id = first.receipt_id
    row_id = _store_row_id(BATCH_ID)
    good_title = next(
        r for r in batch.records if r.source_url == GOOD_URL
    ).fields["title"]

    # 同一次更正再发一遍（输入完全相同 → 相同重放签名）
    second = replay_failed_batch(batch, _make_rule(), _make_clean_rules())

    assert second.receipt_id == first_receipt_id, "重发应复用同一份回执"
    assert second.signature == first.signature
    assert batch.active_recovery_versions == 1, "批次仍只有一个有效的恢复版本"

    # 存储清单仍是同一行、同一份回执，没有追加重复批次行
    assert _store_row_id(BATCH_ID) == row_id
    row = _find_store_row(BATCH_ID)
    assert row["失败批更正回执"]["receipt_id"] == first_receipt_id
    assert row["有效恢复版本数"] == 1

    # 原批另一篇文章始终未被替换
    good_record = next(r for r in batch.records if r.source_url == GOOD_URL)
    assert good_record.fields["title"] == good_title == "同批另一篇通过校验的文章"


def test_replay_compatible_with_already_persisted_batch_row() -> None:
    """中断后重试：历史批次已经落库时，重放原地更新而不是新增行。"""
    batch = _make_batch()
    # 模拟中断前已经落库的历史行
    store.rows("batch_store").append({
        "id": 7001, "批次编号": BATCH_ID, "来源作业": batch.source_job_id,
        "数据行数": 2, "存储路径": batch.storage_path, "压缩格式": "gzip",
        "Schema版本": "v1", "校验摘要": "checksum-before-correction",
        "入库时间": "2026-11-20T00:00:00+00:00",
        "status": "校验失败", "pending": False, "abnormal": True,
    })
    register_captured_response(batch, _original_response())

    replay_failed_batch(batch, _make_rule(), _make_clean_rules())

    matching = [
        row for row in store.rows("batch_store")
        if str(row.get("批次编号")) == BATCH_ID
    ]
    assert len(matching) == 1, "已落库行应原地更新，不得新增重复批次行"
    assert matching[0]["id"] == 7001
    assert matching[0]["status"] == "校验通过"
    assert matching[0]["校验摘要"] == batch.checksum[:16]


def test_resubmit_after_failed_attempt_reuses_receipt_once_identical() -> None:
    """失败重跑后以相同输入再发，同样不会制造第二个版本；换输入才生成新回执。"""
    batch = _make_batch()
    register_captured_response(batch, _no_response_snapshot())

    first = replay_failed_batch(batch, _make_rule(), _make_clean_rules())
    second = replay_failed_batch(batch, _make_rule(), _make_clean_rules())
    assert second.receipt_id == first.receipt_id
    assert batch.active_recovery_versions == 0

    # 随后找回真实原批响应，输入改变，产生新回执并完成唯一一次有效恢复
    register_captured_response(batch, _original_response())
    third = replay_failed_batch(batch, _make_rule(), _make_clean_rules())
    assert third.receipt_id != first.receipt_id
    assert third.overall_passed is True
    assert batch.active_recovery_versions == 1
    assert batch.status == "校验通过"
