from dataclasses import dataclass
from datetime import datetime, timezone

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


def add_batch_metadata(df: pd.DataFrame, batch: BatchMetadata) -> pd.DataFrame:
    """DataFrame에 batch_job, batch_date 컬럼 추가."""
    if df.empty:
        return df
    df["batch_job"] = batch.batch_job
    df["batch_date"] = pd.Timestamp(batch.batch_date)
    return df
