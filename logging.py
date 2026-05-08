"""
Common pipeline logging helpers.

The default unit is a job-level lifecycle log:
    [START] job=graph_sync run_id=20260506_1200
    [END] job=graph_sync status=SUCCESS duration=43s
    [FAIL] job=graph_sync task=upsert_edges error=timeout

Processing, data error, and audit logs use the same key=value shape:
    [PROCESS] job=graph_sync run_id=20260506_1200 pending_events=1200 ...
    [DATA_ERROR] level=ERROR job=ingredient_mapping run_id=20260506_1200 ...
    [AUDIT] run_id=20260506_1200 job_version=v1 git_commit_hash=abc123 ...

Set LOG_FORMAT=json to emit JSON Lines for CloudWatch Insights.
Set LOG_LEVEL to control verbosity (default: INFO).
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Optional


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for CloudWatch Insights."""

    def __init__(self, service: str = "4evr0") -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        ms = int(record.msecs)
        timestamp = dt.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}Z")

        obj: dict[str, Any] = {
            "timestamp": timestamp,
            "level": record.levelname,
            "logger": record.name,
            "service": self._service,
        }

        structured: dict[str, Any] | None = getattr(record, "structured", None)
        if structured:
            obj.update(structured)
        else:
            obj["message"] = record.getMessage()

        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)

        safe: dict[str, Any] = {}
        for k, v in obj.items():
            try:
                json.dumps(v)
                safe[k] = v
            except (TypeError, ValueError):
                safe[k] = str(v)

        return json.dumps(safe, ensure_ascii=False)


def setup_logging(service_name: str = "4evr0") -> None:
    """Configure root logger. Idempotent — safe to call multiple times.

    LOG_FORMAT=json  → JSON Lines via StreamHandler(stdout)
    LOG_FORMAT=<any> → human-readable text (default)
    LOG_LEVEL        → log level name (default: INFO)
    """
    log_level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    use_json = os.environ.get("LOG_FORMAT", "").lower() == "json"

    root = logging.getLogger()
    root.setLevel(log_level)

    # Idempotency: skip if a compatible handler is already attached
    for h in root.handlers:
        if isinstance(h, logging.StreamHandler):
            if use_json and isinstance(h.formatter, JsonFormatter):
                return
            if not use_json and not isinstance(h.formatter, JsonFormatter):
                return

    if use_json:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter(service=service_name))
    else:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ))

    root.addHandler(handler)


def log_job_start(
    logger: logging.Logger,
    job: str,
    run_id: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log the start of a job unit."""
    struct = _build_struct("START", job=job, run_id=run_id, **fields)
    logger.info(_format_log("START", job=job, run_id=run_id, **fields), extra={"structured": struct})


def log_job_end(
    logger: logging.Logger,
    job: str,
    status: str = "SUCCESS",
    duration: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log the successful end of a job unit."""
    struct = _build_struct("END", job=job, status=status, duration=duration, **fields)
    logger.info(_format_log("END", job=job, status=status, duration=duration, **fields), extra={"structured": struct})


def log_job_fail(
    logger: logging.Logger,
    job: str,
    task: Optional[str] = None,
    error: Optional[Any] = None,
    **fields: Any,
) -> None:
    """Log a failed job unit."""
    struct = _build_struct(
        "FAIL",
        job=job,
        task=task,
        error=str(error) if error is not None else None,
        **fields,
    )
    logger.error(_format_log("FAIL", job=job, task=task, error=error, **fields), extra={"structured": struct})


def log_process_summary(
    logger: logging.Logger,
    job: str,
    run_id: str,
    pending_events: Optional[int] = None,
    processed_events: Optional[int] = None,
    failed_events: Optional[int] = None,
    upserted_nodes: Optional[int] = None,
    upserted_edges: Optional[int] = None,
    deactivated_edges: Optional[int] = None,
    duration: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log a data processing summary for one job run."""
    struct = _build_struct(
        "PROCESS",
        job=job,
        run_id=run_id,
        pending_events=pending_events,
        processed_events=processed_events,
        failed_events=failed_events,
        upserted_nodes=upserted_nodes,
        upserted_edges=upserted_edges,
        deactivated_edges=deactivated_edges,
        duration=duration,
        **fields,
    )
    logger.info(
        _format_log(
            "PROCESS",
            job=job,
            run_id=run_id,
            pending_events=pending_events,
            processed_events=processed_events,
            failed_events=failed_events,
            upserted_nodes=upserted_nodes,
            upserted_edges=upserted_edges,
            deactivated_edges=deactivated_edges,
            duration=duration,
            **fields,
        ),
        extra={"structured": struct},
    )


def log_data_error(
    logger: logging.Logger,
    job: str,
    run_id: str,
    table: str,
    row_id: Any,
    column: str,
    value: Any,
    error_type: str,
    message: str,
    **fields: Any,
) -> None:
    """Log a row-level data processing error."""
    struct = _build_struct(
        "DATA_ERROR",
        level="ERROR",
        job=job,
        run_id=run_id,
        table=table,
        row_id=row_id,
        column=column,
        value=str(value),
        error_type=error_type,
        message=message,
        **fields,
    )
    logger.error(
        _format_log(
            "DATA_ERROR",
            level="ERROR",
            job=job,
            run_id=run_id,
            table=table,
            row_id=row_id,
            column=column,
            value=_quoted(value),
            error_type=error_type,
            message=_quoted(message),
            **fields,
        ),
        extra={"structured": struct},
    )


def log_audit(
    logger: logging.Logger,
    run_id: str,
    job_version: str,
    git_commit_hash: str,
    source_table: str,
    source_snapshot_id: Any,
    target_table: str,
    target_snapshot_id: Any,
    created_at: Optional[str] = None,
    created_by: Optional[str] = None,
    **fields: Any,
) -> None:
    """Log an audit event for table lineage and reproducibility."""
    _created_at = created_at or _utc_now()
    struct = _build_struct(
        "AUDIT",
        run_id=run_id,
        job_version=job_version,
        git_commit_hash=git_commit_hash,
        source_table=source_table,
        source_snapshot_id=source_snapshot_id,
        target_table=target_table,
        target_snapshot_id=target_snapshot_id,
        created_at=_created_at,
        created_by=created_by,
        **fields,
    )
    logger.info(
        _format_log(
            "AUDIT",
            run_id=run_id,
            job_version=job_version,
            git_commit_hash=git_commit_hash,
            source_table=source_table,
            source_snapshot_id=source_snapshot_id,
            target_table=target_table,
            target_snapshot_id=target_snapshot_id,
            created_at=_created_at,
            created_by=created_by,
            **fields,
        ),
        extra={"structured": struct},
    )


@contextmanager
def job_unit(
    logger: logging.Logger,
    job: str,
    run_id: Optional[str] = None,
    task: Optional[str] = None,
    **fields: Any,
) -> Iterator[None]:
    """Log START/END/FAIL around a job block. Exceptions are re-raised after FAIL."""
    started_at = time.monotonic()
    log_job_start(logger, job=job, run_id=run_id, **fields)
    try:
        yield
    except Exception as exc:
        log_job_fail(logger, job=job, task=task, error=exc)
        raise
    else:
        log_job_end(
            logger,
            job=job,
            status="SUCCESS",
            duration=_format_duration(time.monotonic() - started_at),
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_struct(event: str, **fields: Any) -> dict[str, Any]:
    return {k: v for k, v in {"event": event, **fields}.items() if v is not None}


def _format_log(event: str, **fields: Any) -> str:
    pairs = " ".join(
        f"{key}={_format_value(value)}"
        for key, value in fields.items()
        if value is not None
    )
    if pairs:
        return f"[{event}] {pairs}"
    return f"[{event}]"


def _format_value(value: Any) -> str:
    if isinstance(value, _RawValue):
        return str(value)
    text = str(value)
    if not text:
        return '""'
    if any(char.isspace() for char in text):
        return json.dumps(text, ensure_ascii=False)
    return text


def _format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds:.3f}s"
    return f"{seconds:.0f}s"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class _RawValue(str):
    pass


def _quoted(value: Any) -> _RawValue:
    return _RawValue(json.dumps(str(value), ensure_ascii=False))
