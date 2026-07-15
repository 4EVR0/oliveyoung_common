import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class BatchMetadata:
    batch_job: str      # 파이프라인 이름 e.g. "oliveyoung_crawl", "bronze_to_silver"
    batch_date: datetime

    @property
    def run_id(self) -> str:
        return f"{self.batch_job}_{self.batch_date.strftime('%Y%m%d_%H%M%S')}"


def create_batch_metadata(job: str) -> BatchMetadata:
    """UTC 기준 BatchMetadata 생성."""
    batch_date = datetime.now(timezone.utc)
    return BatchMetadata(batch_job=job, batch_date=batch_date)


def build_run_id(job: str) -> str:
    """run_id 문자열만 필요할 때. 형식: {job}_{YYYYMMDD_HHMMSS}"""
    return create_batch_metadata(job).run_id


def batch_date_from_run_id(run_id: str) -> str:
    """run_id/ds 문자열에서 논리 배치 날짜(YYYY-MM-DD) 추출.

    예) '20260710' 또는 'oliveyoung_crawl_20260710_153042' → '2026-07-10'.
    단독 실행 시 소스 bronze run_id에서 batch_date를 파생할 때 쓴다.
    """
    m = re.search(r"(\d{4})(\d{2})(\d{2})", run_id)
    if not m:
        raise ValueError(f"run_id에서 날짜를 찾을 수 없음: {run_id}")
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def add_batch_metadata(
    df: pd.DataFrame,
    batch: BatchMetadata,
    batch_date: Optional[str] = None,
) -> pd.DataFrame:
    """DataFrame에 batch_job, batch_date 컬럼 추가.

    batch_date(YYYY-MM-DD)를 주면 단계 관통 논리 날짜(자정 UTC)를 찍고,
    없으면 batch.batch_date(벽시계)로 폴백.
    """
    if df.empty:
        return df
    df["batch_job"] = batch.batch_job
    if batch_date:
        df["batch_date"] = pd.Timestamp(batch_date, tz="UTC")
    else:
        df["batch_date"] = pd.Timestamp(batch.batch_date)
    return df
