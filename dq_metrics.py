"""
정합성(DQ) 메트릭 테이블 계약 — 공유 모듈

파이프라인 각 단계가 log_dq로 남기는 정합성 수치를 Iceberg 테이블
(oliveyoung_db.dq_metrics)에 지표당 1행씩 append한다.

key/value(EAV) 형태라 지표를 추가해도 스키마를 바꿀 필요가 없다.
log_dq(로그·Loki)와 write_dq_metrics(테이블)는 같은 kwargs를 받으므로
호출부에서 나란히 쓰면 로그와 테이블이 어긋나지 않는다.

- 스키마/테이블명: 이 파일이 단일 출처
- write_dq_metrics: catalog를 인자로 받는 순수 함수 → 각 레포가 자기 카탈로그로 호출
- create_dq_metrics_table: warehouse 접근권 있는 쪽(Oliveyoung_Pipeline)에서 1회 실행

pyiceberg/pyarrow에 의존하므로, 이 모듈을 import하는 소비 레포는 두 패키지가 필요하다.
(logging/batch만 쓰는 크롤러는 이 모듈을 import하지 않는 한 영향 없음)
"""

import json
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType, TimestamptzType
from pyiceberg.table.sorting import SortOrder, SortField, SortDirection, NullOrder
from pyiceberg.transforms import IdentityTransform


DQ_METRICS_TABLE = "oliveyoung_db.dq_metrics"

# key/value(EAV) — 지표당 1행. 지표 추가 시 스키마 변경 불필요.
DQ_METRICS_SCHEMA = Schema(
    NestedField(1, "batch_date",   StringType(),      required=True),   # YYYY-MM-DD, 단계 관통 논리 배치 날짜(조인·드릴다운 키)
    NestedField(2, "run_id",       StringType(),      required=False),  # 초단위 유니크 실행 식별(추적용)
    NestedField(3, "stage",        StringType(),      required=False),  # crawl | bronze_to_silver | silver_to_gold
    NestedField(4, "metric_name",  StringType(),      required=False),  # match_rate, error_rate, bronze_loaded ...
    NestedField(5, "metric_value", DoubleType(),      required=False),  # 카운트도 double(5282.0), 비율도 double
    NestedField(6, "target_table", StringType(),      required=False),  # 드릴다운 대상 테이블(없으면 null)
    NestedField(7, "created_at",   TimestamptzType(), required=False),
)

# 최신 run이 위로 오도록 created_at DESC 정렬
DQ_METRICS_SORT = SortOrder(
    SortField(
        source_id=7, transform=IdentityTransform(),
        direction=SortDirection.DESC, null_order=NullOrder.NULLS_LAST,
    )
)


def create_dq_metrics_table(catalog, location: Optional[str] = None) -> bool:
    """dq_metrics 테이블을 생성한다(이미 있으면 건너뜀).

    warehouse 접근권이 있는 쪽(Oliveyoung_Pipeline)에서 1회 실행.
    Returns: 새로 생성했으면 True, 이미 있으면 False.
    """
    kwargs = {"location": location} if location else {}
    try:
        catalog.create_table(
            identifier=DQ_METRICS_TABLE,
            schema=DQ_METRICS_SCHEMA,
            sort_order=DQ_METRICS_SORT,
            **kwargs,
        )
        return True
    except Exception as e:
        if "AlreadyExists" in type(e).__name__ or "already exists" in str(e).lower():
            return False
        raise


def write_dq_metrics(
    catalog,
    stage: str,
    batch_date: str,
    run_id: str,
    target_table: Optional[str] = None,
    created_at: Optional[datetime] = None,
    report_webhook: Optional[str] = None,
    **metrics: Any,
) -> None:
    """단계의 정합성 수치(**metrics)를 dq_metrics에 지표당 1행씩 append한다.

    숫자형 지표만 저장한다 — 문자열 등(예: code_version)은 로그로만 남기고 건너뛴다.
    report_webhook을 주면 append 성공 후 Discord로 완료 리포트를 보낸다(옵트인·비치명적).
    안 주면 부수효과 없이 기존과 동일하게 동작.
    """
    ts = created_at or datetime.now(timezone.utc)

    names: list[str] = []
    values: list[float] = []
    for name, value in metrics.items():
        # bool은 int의 하위형이라 명시적으로 제외
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        names.append(name)
        values.append(float(value))

    if not names:
        return

    n = len(names)
    table = catalog.load_table(DQ_METRICS_TABLE)
    arrow = pa.table(
        {
            "batch_date":   pa.array([batch_date] * n,    type=pa.string()),
            "run_id":       pa.array([run_id] * n,        type=pa.string()),
            "stage":        pa.array([stage] * n,         type=pa.string()),
            "metric_name":  pa.array(names,               type=pa.string()),
            "metric_value": pa.array(values,              type=pa.float64()),
            "target_table": pa.array([target_table] * n,  type=pa.string()),
            "created_at":   pa.array([ts] * n,            type=pa.timestamp("us", tz="UTC")),
        },
        schema=table.schema().as_arrow(),
    )
    table.append(arrow)

    # 적재 성공 후에만 완료 리포트 전송(옵트인) — 실패한 배치가 "완료"로 오보되지 않게
    if report_webhook:
        _send_report(report_webhook, stage, batch_date, names, values)


# ── 완료 리포트(Discord) ────────────────────────────────────────────────────
# stage → (파이프라인 표기명, 완료 액션어). 전처리 2단계를 액션어로 구분.
_STAGE_META = {
    "crawl":            ("올리브영 크롤링", "수집 완료"),
    "bronze_to_silver": ("올리브영 전처리", "정제 완료"),
    "silver_to_gold":   ("올리브영 전처리", "성분 매칭 완료"),
}

# metric_name → (이모지, 라벨, 포맷). 대시보드와 통일: rate=%(percentunit), 카운트=,건.
_METRIC_LABELS = {
    "products_total":        ("📦", "수집 상품",   "count"),
    "categories_total":      ("📂", "카테고리",    "count"),
    "categories_failed":     ("⚠️", "실패",       "count"),
    "categories_zero":       ("🕳️", "빈 카테고리",  "count"),
    "bronze_loaded":         ("📥", "bronze 로드", "count"),
    "silver_ok":             ("✅", "정상",       "count"),
    "silver_error":          ("⚠️", "에러",       "count"),
    "error_rate":            ("📊", "오류율",     "pct"),
    "ingredients_unique":    ("🧪", "고유 성분",   "count"),
    "ingredients_matched":   ("🔗", "INCI 매칭",  "count"),
    "ingredients_unmatched": ("❔", "미매칭",      "count"),
    "match_rate":            ("📊", "매칭율",     "pct"),
}

_DASHBOARD_URL = "http://43.200.169.27:3000/d/oliveyoung-dq-table"


def _fmt_metric(kind: str, v: float) -> str:
    """리포트용 값 포맷 — rate는 %, 정수 카운트는 천단위+건."""
    if kind == "pct":
        return f"{v * 100:.1f}%"
    if v == int(v):
        return f"{int(v):,}건"
    return f"{v:g}"


def _send_report(webhook: str, stage: str, batch_date: str,
                 names: list[str], values: list[float]) -> None:
    """단계 완료 리포트를 Discord 웹훅으로 전송한다(실패해도 조용히 무시)."""
    try:
        pipeline, action = _STAGE_META.get(stage, (stage, "완료"))
        lines = [
            f"✅ **[{pipeline}] {action}**",
            "━" * 20,
            f"📅 배치   {batch_date}",
        ]
        for name, value in zip(names, values):
            emoji, label, kind = _METRIC_LABELS.get(name, ("•", name, "count"))
            lines.append(f"{emoji} {label}   {_fmt_metric(kind, value)}")
        lines.append(f"🔗 [대시보드]({_DASHBOARD_URL})")

        body = json.dumps({"content": "\n".join(lines)}).encode("utf-8")
        req = urllib.request.Request(
            webhook, data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # 리포트는 부가 기능 — 적재/파이프라인을 절대 깨지 않음
