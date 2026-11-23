"""失败批更正重放：基于实际采集响应与抽取/清洗规则做清洗重放。

修复点：
- 不再用 ``recovered:<字段>`` 之类的占位值伪造通过，恢复后摘要必须来自实际网页响应；
- 无可用响应时只保留未通过原因，占位结果一律不通过；
- 原批响应与补采响应标题冲突时按固定优先级裁决，且全过程留痕可回查；
- 同一次更正重发幂等：批次内始终只有一个有效的恢复版本；
- 重放结果同步回批次存储已落库行，同批其他网页的标题和摘要不丢失；
- 批次存储清单与重放明细都从同一份失败批更正回执取数。
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from app.integration import RawExtractedContent, _apply_clean_rules
from app.models import (
    CapturedResponse, CleanRule, CleanedContent, ExtractRule,
    FailureCorrectionReceipt, ReplayLineResult, StorageBatch,
)
from app.store import store

# 选源优先级：原批证据优先于后续补采，补采拿到的新标题不能擅自覆盖入库标题
DEFAULT_PRIORITY: list[str] = ["original_capture", "supplemental"]

# 按实际响应正文做字段抽取的规则锚点（与 ExtractRule 的字段映射配套，可回查规则编号）
_FIELD_PATTERNS: dict[str, re.Pattern[str]] = {
    "title": re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL),
    "h1": re.compile(r"<h1[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL),
    "body": re.compile(r"<body[^>]*>(.*?)</body>", re.IGNORECASE | re.DOTALL),
    "publish_date": re.compile(
        r"(?:publish_date|datePublished|发布时间)[:\s]*"
        r"([0-9]{4}-[0-9]{2}-[0-9]{2}(?:[ T][0-9:]{4,8})?)",
        re.IGNORECASE,
    ),
}
_TAG_STRIP = re.compile(r"<[^>]+>")


def register_captured_response(batch: StorageBatch, snapshot: CapturedResponse) -> None:
    """登记值班员找回的原批响应或后续补采响应，作为重放回查的实际输入。"""
    bucket = batch.response_snapshots.setdefault(snapshot.source_url, [])
    for existing in bucket:
        if existing.snapshot_id == snapshot.snapshot_id:
            return
    bucket.append(snapshot)


def replay_failed_batch(
    batch: StorageBatch,
    extract_rule: ExtractRule,
    clean_rules: list[CleanRule],
    *,
    priority: list[str] | None = None,
    sync_store: bool = True,
) -> FailureCorrectionReceipt | None:
    """对失败批次按实际留存响应做清洗重放，返回失败批更正回执。

    只重放批次内未通过的行；已通过的兄弟行原样保留。无响应的行不会被伪造为
    通过，未通过原因写入回执。同一次更正（相同重放签名）重发时复用既有回执，
    不增加有效恢复版本数。
    """
    priority = list(priority or DEFAULT_PRIORITY)
    failed_records = [r for r in batch.records if not r.is_valid()]

    # 幂等重发：失败行已被上一轮更正修复时，直接回传同一份回执，不产生新版本
    if not failed_records:
        if batch.correction_receipt is not None:
            if sync_store:
                _sync_batch_store_row(batch)
            return batch.correction_receipt
        return None
    if batch.status != "校验失败":
        return None

    previous_checksum = batch.checksum
    now = datetime.now(timezone.utc)
    lines: list[ReplayLineResult] = []
    conflicts: list[dict[str, Any]] = []

    for record in failed_records:
        line = _replay_one_line(record, batch, extract_rule, clean_rules, priority, now)
        lines.append(line)
        if line.passed and line.chosen_snapshot_id:
            # 只覆盖失败行；同批其他网页的标题和摘要保持原值
            record.fields = dict(line.fields)
            record.clean_errors = []
            record.provenance = {
                "source_snapshot_id": line.chosen_snapshot_id,
                "source_origin": line.chosen_origin,
                "source_origin_label": line.chosen_origin_label,
                "http_status": line.evidence.get("http_status"),
                "extract_rule_id": extract_rule.rule_id,
                "clean_rule_ids": [rule.rule_id for rule in clean_rules],
                "replayed_at": now.isoformat(),
                "receipt_line": True,
            }
        # 未通过行不写任何占位值，失败原因只在回执中留存

    for conflict in _detect_title_conflicts(batch, failed_records, extract_rule, priority):
        conflicts.append(conflict)

    overall_passed = all(line.passed for line in lines)
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(r.is_valid() for r in batch.records) else "校验失败"
    batch.stored_at = now.isoformat()
    batch.row_count = len(batch.records)

    signature = _build_signature(batch.batch_id, lines)
    if batch.correction_receipt is not None and batch.correction_receipt.signature == signature:
        # 同一次更正再发一遍：复用既有回执，批次仍只有一个有效恢复版本
        receipt = batch.correction_receipt
    else:
        receipt = FailureCorrectionReceipt(
            receipt_id=f"FCR-{batch.batch_id}-{now.strftime('%Y%m%d%H%M%S%f')}",
            batch_id=batch.batch_id,
            replayed_at=now.isoformat(),
            overall_passed=overall_passed,
            previous_checksum=previous_checksum,
            corrected_checksum=batch.checksum,
            signature=signature,
            lines=lines,
            title_conflicts=conflicts,
            priority=list(priority),
        )
        batch.correction_receipt = receipt
        # 只有真正通过的更正才计入有效恢复版本，失败的占位重跑不计
        if overall_passed:
            batch.active_recovery_versions += 1

    if sync_store:
        _sync_batch_store_row(batch)
    return receipt


def _replay_one_line(
    record: CleanedContent,
    batch: StorageBatch,
    extract_rule: ExtractRule,
    clean_rules: list[CleanRule],
    priority: list[str],
    now: datetime,
) -> ReplayLineResult:
    url = record.source_url
    snapshots = list(batch.response_snapshots.get(url, []))
    available = [s for s in snapshots if s.available and s.body]
    rejected = [_rejected_snapshot(s) for s in snapshots if s not in available]
    rule_evidence = {
        "extract_rule_id": extract_rule.rule_id,
        "clean_rule_ids": [rule.rule_id for rule in clean_rules],
        "all_snapshot_ids": [s.snapshot_id for s in snapshots],
        "rejected_snapshots": rejected,
    }

    if not available:
        if snapshots:
            detail = "；".join(
                f"{s.origin_label}({s.failure_reason or f'HTTP {s.http_status}'})"
                for s in snapshots
            )
        else:
            detail = "批次未留存任何实际响应快照，禁止使用 recovered 占位值恢复"
        return ReplayLineResult(
            source_url=url,
            passed=False,
            fields=dict(record.fields),
            clean_errors=list(record.clean_errors),
            failure_reason=f"无可用实际响应，重放不通过：{detail}",
            evidence=rule_evidence,
        )

    order = {name: index for index, name in enumerate(priority)}
    ranked = sorted(
        available,
        key=lambda snap: (order.get(snap.origin, len(order)), snap.captured_at),
    )
    chosen = ranked[0]

    # 从实际响应正文抽取（按抽取规则的字段映射），再走同一套清洗规则重放
    raw_fields = _extract_from_response(chosen, extract_rule)
    raw = RawExtractedContent(
        source_url=url,
        extract_rule_id=extract_rule.rule_id,
        raw_fields=raw_fields,
        errors=[],
    )
    cleaned = _apply_clean_rules(raw, clean_rules)
    passed = cleaned.is_valid()
    if passed:
        failure_reason = ""
    else:
        reason = "；".join(cleaned.clean_errors) if cleaned.clean_errors else "抽取字段缺失或为空"
        failure_reason = f"按实际响应重放后仍未通过：{reason}"

    return ReplayLineResult(
        source_url=url,
        passed=passed,
        fields=dict(cleaned.fields),
        clean_errors=list(cleaned.clean_errors),
        failure_reason=failure_reason,
        chosen_snapshot_id=chosen.snapshot_id,
        chosen_origin=chosen.origin,
        evidence={
            **rule_evidence,
            "http_status": chosen.http_status,
            "captured_at": chosen.captured_at,
            "candidate_titles": _candidate_titles(available, extract_rule),
            "raw_fields_from_response": raw_fields,
            "replayed_at": now.isoformat(),
        },
    )


def _detect_title_conflicts(
    batch: StorageBatch,
    failed_records: list[CleanedContent],
    extract_rule: ExtractRule,
    priority: list[str],
) -> list[dict[str, Any]]:
    """旧响应与补采响应标题不一致时记录冲突及按优先级的裁决结果。"""
    conflicts: list[dict[str, Any]] = []
    order = {name: index for index, name in enumerate(priority)}
    for record in failed_records:
        snapshots = [
            snap for snap in batch.response_snapshots.get(record.source_url, [])
            if snap.available and snap.body
        ]
        candidates: list[dict[str, Any]] = []
        for snap in snapshots:
            title = _extract_from_response(snap, extract_rule).get("title", "").strip()
            if title:
                candidates.append({
                    "snapshot_id": snap.snapshot_id,
                    "origin": snap.origin,
                    "origin_label": snap.origin_label,
                    "title": title,
                })
        distinct = {item["title"] for item in candidates}
        if len(distinct) <= 1:
            continue
        ranked = sorted(
            snapshots,
            key=lambda snap: (order.get(snap.origin, len(order)), snap.captured_at),
        )
        chosen = ranked[0]
        chosen_title = next(
            (item["title"] for item in candidates if item["snapshot_id"] == chosen.snapshot_id),
            "",
        )
        conflicts.append({
            "source_url": record.source_url,
            "chosen_snapshot_id": chosen.snapshot_id,
            "chosen_origin": chosen.origin,
            "chosen_origin_label": chosen.origin_label,
            "chosen_title": chosen_title,
            "candidates": candidates,
            "resolution": (
                f"标题冲突按优先级 {' > '.join(priority)} 裁决，"
                f"入库标题取自{chosen.origin_label}，补采新标题不覆盖原批证据"
            ),
        })
    return conflicts


def _extract_from_response(snapshot: CapturedResponse, rule: ExtractRule) -> dict[str, str]:
    """按抽取规则的字段映射，从实际响应正文抽取原始字段（清洗前）。"""
    target_fields: list[str] = []
    for value in rule.field_mapping.values():
        target_fields.extend(part.strip() for part in str(value).split(",") if part.strip())
    if not target_fields:
        target_fields = list(_FIELD_PATTERNS)

    fields: dict[str, str] = {}
    for name in target_fields:
        pattern = _FIELD_PATTERNS.get(name)
        if pattern is None:
            continue
        match = pattern.search(snapshot.body or "")
        if match:
            fields[name] = _TAG_STRIP.sub("", match.group(1)).strip()
    return fields


def _candidate_titles(snapshots: list[CapturedResponse], rule: ExtractRule) -> list[dict[str, Any]]:
    titles: list[dict[str, Any]] = []
    for snap in snapshots:
        titles.append({
            "snapshot_id": snap.snapshot_id,
            "origin": snap.origin,
            "origin_label": snap.origin_label,
            "title": _extract_from_response(snap, rule).get("title", ""),
        })
    return titles


def _rejected_snapshot(snapshot: CapturedResponse) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot.snapshot_id,
        "origin": snapshot.origin,
        "origin_label": snapshot.origin_label,
        "reason": snapshot.failure_reason or (
            "无响应正文" if not snapshot.body else f"HTTP {snapshot.http_status}"
        ),
    }


def _build_signature(batch_id: str, lines: list[ReplayLineResult]) -> str:
    payload = json.dumps(
        [
            {
                "url": line.source_url,
                "snapshot": line.chosen_snapshot_id or "none",
                "fields": line.fields,
                "failure_reason": line.failure_reason,
            }
            for line in lines
        ],
        sort_keys=True,
        ensure_ascii=False,
    )
    digest = hashlib.sha256(f"{batch_id}|{payload}".encode("utf-8")).hexdigest()
    return f"RPL-{digest[:16]}"


def _sync_batch_store_row(batch: StorageBatch) -> dict[str, Any] | None:
    """把重放结果同步到批次存储清单；存储明细只从同一份更正回执取数。

    历史批次已落库时原地更新，不追加重复行；尚未落库时补登记一行。
    """
    rows = store.rows("batch_store")
    receipt = batch.correction_receipt
    receipt_dict = receipt.to_dict() if receipt is not None else None
    payload = {
        "数据行数": batch.row_count,
        "存储路径": batch.storage_path,
        "压缩格式": batch.compression,
        "Schema版本": batch.schema_version,
        "校验摘要": batch.checksum[:16],
        "入库时间": batch.stored_at,
        "status": batch.status,
        "pending": batch.status not in ("校验通过", "校验失败"),
        "abnormal": batch.status == "校验失败",
        "失败批更正回执": receipt_dict,
        "有效恢复版本数": batch.active_recovery_versions,
        # 明细与摘要均取自同一份回执，保证存储清单与重放结果一致
        "重放明细": receipt_dict["lines"] if receipt_dict else [],
        "标题冲突裁决": receipt_dict["title_conflicts"] if receipt_dict else [],
        "来源快照": {
            url: [snapshot.to_dict() for snapshot in snaps]
            for url, snaps in batch.response_snapshots.items()
        },
    }

    for row in rows:
        if str(row.get("批次编号")) == batch.batch_id:
            row.update(payload)
            return row

    new_row = {
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "批次编号": batch.batch_id,
        "来源作业": batch.source_job_id,
        **payload,
    }
    rows.append(new_row)
    return new_row
