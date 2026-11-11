"""Serialized, file-backed migration metrics storage."""

from concurrent.futures import Future
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import queue
import re
import threading
import uuid


_DB_PATH = Path(os.environ.get(
    "DATABRIDGE_METRICS_DB_PATH",
    str(Path(__file__).parent / ".migration_state" / "metrics.duckdb"),
))
_FIELDS = {
    "current_stage", "status", "end_time", "duration_seconds", "rows_read",
    "rows_written", "rows_rejected", "bytes_processed", "watermark",
    "validation_result", "error_details",
}
_SENSITIVE_ERROR_PATTERNS = (
    (re.compile(r"(?i)(aws_access_key_id|access[_ -]?key|secret(?:[_ -]?access)?[_ -]?key|session[_ -]?token|password|token)\s*[:=]\s*(['\"]?)[^\s,'\";]+"),
     r"\1=[REDACTED]"),
    (re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"), "[REDACTED_AWS_KEY]"),
    (re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*"), "Bearer [REDACTED]"),
    (re.compile(r"(?i)(https?://)[^/@\s:]+:[^/@\s]+@"), r"\1[REDACTED]@"),
)


def sanitize_error(error, secrets=()):
    """Reduce an exception to bounded, credential-redacted text."""
    text = " ".join(str(error or "").split())
    for secret in secrets:
        if secret:
            text = text.replace(str(secret), "[REDACTED]")
    for pattern, replacement in _SENSITIVE_ERROR_PATTERNS:
        text = pattern.sub(replacement, text)
    return text[:1000]


def _now():
    return datetime.now(timezone.utc).isoformat()


class MetricsRepository:
    """Own one DuckDB connection and serialize all operations on a writer thread."""

    def __init__(self, path=None):
        self.path = Path(path or _DB_PATH).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._requests = queue.Queue()
        self._ready = Future()
        self._thread = threading.Thread(
            target=self._writer_loop, name="databridge-metrics-writer", daemon=True
        )
        self._thread.start()
        self._ready.result()

    def _writer_loop(self):
        import duckdb

        connection = None
        try:
            connection = duckdb.connect(str(self.path))
            connection.execute("""
                CREATE TABLE IF NOT EXISTS migration_runs (
                    run_id VARCHAR PRIMARY KEY,
                    source VARCHAR NOT NULL,
                    target VARCHAR NOT NULL,
                    start_time VARCHAR NOT NULL,
                    end_time VARCHAR,
                    duration_seconds DOUBLE,
                    status VARCHAR NOT NULL,
                    current_stage VARCHAR NOT NULL,
                    rows_read BIGINT NOT NULL DEFAULT 0,
                    rows_written BIGINT NOT NULL DEFAULT 0,
                    rows_rejected BIGINT,
                    bytes_processed BIGINT NOT NULL DEFAULT 0,
                    watermark VARCHAR,
                    validation_result VARCHAR,
                    error_details VARCHAR,
                    updated_at VARCHAR NOT NULL
                )
            """)
            connection.execute("""
                CREATE TABLE IF NOT EXISTS migration_attempts (
                    attempt_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    job_id VARCHAR NOT NULL,
                    operation VARCHAR NOT NULL DEFAULT 'pipeline',
                    source VARCHAR NOT NULL,
                    target VARCHAR NOT NULL,
                    start_time VARCHAR NOT NULL,
                    end_time VARCHAR,
                    duration_seconds DOUBLE,
                    status VARCHAR NOT NULL,
                    current_stage VARCHAR NOT NULL,
                    rows_read BIGINT,
                    rows_written BIGINT,
                    rows_rejected BIGINT,
                    bytes_processed BIGINT,
                    watermark VARCHAR,
                    validation_result VARCHAR,
                    error_details VARCHAR
                )
            """)
            connection.execute(
                """ALTER TABLE migration_attempts
                   ADD COLUMN IF NOT EXISTS operation VARCHAR DEFAULT 'pipeline'"""
            )
            now = _now()
            connection.execute(
                """UPDATE migration_attempts SET status='interrupted',
                       current_stage='interrupted', end_time=?,
                       duration_seconds=epoch(CAST(? AS TIMESTAMPTZ) - CAST(start_time AS TIMESTAMPTZ))
                   WHERE status='running'""",
                [now, now],
            )
            connection.execute(
                """UPDATE migration_runs SET status='interrupted',
                       current_stage='interrupted', end_time=?,
                       duration_seconds=epoch(CAST(? AS TIMESTAMPTZ) - CAST(start_time AS TIMESTAMPTZ)),
                       updated_at=?
                   WHERE status='running'""",
                [now, now, now],
            )
            self._ready.set_result(None)
        except Exception as exc:
            if connection is not None:
                try:
                    connection.close()
                except Exception as close_exc:
                    exc = RuntimeError(f"{exc}; closing the DuckDB writer failed: {close_exc}")
            self._ready.set_exception(exc)
            return

        while True:
            item = self._requests.get()
            if item is None:
                connection.close()
                return
            operation, future, checkpoint = item
            if not future.set_running_or_notify_cancel():
                continue
            try:
                if checkpoint:
                    connection.execute("CHECKPOINT")
                connection.execute("BEGIN TRANSACTION")
                result = operation(connection)
                connection.execute("COMMIT")
                future.set_result(result)
            except Exception as exc:
                try:
                    connection.execute("ROLLBACK")
                except Exception as rollback_exc:
                    future.set_exception(RuntimeError(
                        f"DuckDB operation failed: {exc}; rollback also failed: {rollback_exc}"
                    ))
                else:
                    future.set_exception(exc)

    def _submit(self, operation, *, checkpoint=False):
        if not self._thread.is_alive():
            raise RuntimeError("The DuckDB metrics writer is not available.")
        future = Future()
        self._requests.put((operation, future, checkpoint))
        return future.result()

    def start_run(self, run_id, source, target, stage="queued"):
        now = _now()

        def write(connection):
            connection.execute(
                """INSERT INTO migration_runs
                   (run_id, source, target, start_time, status, current_stage, updated_at)
                   VALUES (?, ?, ?, ?, 'running', ?, ?)
                   ON CONFLICT (run_id) DO NOTHING""",
                [run_id, source, target, now, stage, now],
            )
            connection.execute(
                """UPDATE migration_runs SET status='running', current_stage=?,
                       end_time=NULL, duration_seconds=NULL, error_details=NULL, updated_at=?
                   WHERE run_id=?""",
                [stage, now, run_id],
            )
        self._submit(write)

    def start_attempt(self, run_id, job_id, source, target, stage="starting",
                      started_at=None, operation="pipeline"):
        attempt_id = uuid.uuid4().hex
        now = started_at or _now()

        def write(connection):
            connection.execute(
                """INSERT INTO migration_attempts
                   (attempt_id, run_id, job_id, operation, source, target, start_time,
                    status, current_stage)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'running', ?)""",
                [attempt_id, run_id, job_id, operation, source, target, now, stage],
            )
            connection.execute(
                "UPDATE migration_runs SET current_stage=?, updated_at=? WHERE run_id=?",
                [stage, now, run_id],
            )
        self._submit(write)
        return attempt_id

    def update_attempt(self, attempt_id, **fields):
        unknown = set(fields) - _FIELDS
        if unknown:
            raise ValueError(f"Unsupported metrics fields: {', '.join(sorted(unknown))}")
        if not fields:
            return
        updates = dict(fields)
        if "error_details" in updates:
            updates["error_details"] = sanitize_error(updates["error_details"])
        columns = list(updates)
        assignments = ", ".join(f"{name}=?" for name in columns)
        values = [updates[name] for name in columns] + [attempt_id]
        def write(connection):
            connection.execute(
                f"UPDATE migration_attempts SET {assignments} WHERE attempt_id=?", values
            )
            if "current_stage" in updates:
                row = connection.execute(
                    "SELECT run_id FROM migration_attempts WHERE attempt_id=?",
                    [attempt_id],
                ).fetchone()
                if row:
                    connection.execute(
                        "UPDATE migration_runs SET current_stage=?, updated_at=? WHERE run_id=?",
                        [updates["current_stage"], _now(), row[0]],
                    )
        self._submit(write)

    def finish_attempt(self, attempt_id, *, status, current_stage, started_at,
                       duration_seconds=None,
                       rows_read=None, rows_written=None, rows_rejected=None,
                       bytes_processed=None, watermark=None, validation_result=None,
                       error_details=None, secrets=()):
        ended_at = _now()
        try:
            started = datetime.fromisoformat(started_at)
            ended = datetime.fromisoformat(ended_at)
            duration = max(0.0, (ended - started).total_seconds())
        except (TypeError, ValueError):
            duration = None
        if duration_seconds is not None:
            duration = max(0.0, float(duration_seconds))
        self.update_attempt(
            attempt_id,
            status=status,
            current_stage=current_stage,
            end_time=ended_at,
            duration_seconds=duration,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            bytes_processed=bytes_processed,
            watermark=watermark,
            validation_result=validation_result,
            error_details=sanitize_error(error_details, secrets) if error_details else None,
        )

    def finish_run(self, run_id, *, status, current_stage, error_details=None, secrets=()):
        now = _now()

        def write(connection):
            connection.execute(
                """WITH latest AS (
                       SELECT *, ROW_NUMBER() OVER (
                           PARTITION BY job_id, operation ORDER BY start_time DESC, attempt_id DESC
                       ) AS attempt_rank
                       FROM migration_attempts WHERE run_id=?
                   )
                   UPDATE migration_runs SET
                     end_time=?, duration_seconds=epoch(CAST(? AS TIMESTAMPTZ) - CAST(start_time AS TIMESTAMPTZ)),
                     status=?, current_stage=?,
                     rows_read=COALESCE((SELECT SUM(rows_read) FROM latest WHERE attempt_rank=1), 0),
                     rows_written=COALESCE((SELECT SUM(rows_written) FROM latest WHERE attempt_rank=1), 0),
                     rows_rejected=(SELECT SUM(rows_rejected) FROM latest WHERE attempt_rank=1),
                     bytes_processed=COALESCE((SELECT SUM(bytes_processed) FROM latest WHERE attempt_rank=1), 0),
                     watermark=(SELECT watermark FROM latest WHERE attempt_rank=1 AND watermark IS NOT NULL
                                ORDER BY start_time DESC LIMIT 1),
                     validation_result=CASE
                         WHEN EXISTS (SELECT 1 FROM latest WHERE attempt_rank=1 AND validation_result='failed')
                             THEN 'failed'
                         WHEN EXISTS (SELECT 1 FROM latest WHERE attempt_rank=1 AND validation_result='passed')
                              AND NOT EXISTS (SELECT 1 FROM latest WHERE attempt_rank=1
                                              AND validation_result NOT IN ('passed', 'failed'))
                             THEN 'passed'
                         ELSE 'not_run'
                     END,
                     error_details=?, updated_at=?
                   WHERE run_id=?""",
                [run_id, now, now, status, current_stage,
                 sanitize_error(error_details, secrets) if error_details else None, now, run_id],
            )
        self._submit(write)

    def run_status(self, run_id):
        return self._submit(lambda connection: connection.execute(
            "SELECT status FROM migration_runs WHERE run_id=?", [run_id]
        ).fetchone())

    def recent_runs(self, limit=20):
        return self._submit(lambda connection: connection.execute(
            """SELECT run_id, source, target, start_time, end_time, duration_seconds,
                      status, current_stage, rows_read, rows_written, rows_rejected,
                      bytes_processed, watermark, validation_result, error_details
               FROM migration_runs ORDER BY start_time DESC LIMIT ?""",
            [max(1, min(int(limit), 100))],
        ).fetchdf().to_dict("records"))

    def attempts_for_run(self, run_id, limit=100):
        return self._submit(lambda connection: connection.execute(
            """SELECT attempt_id, job_id, operation, source, target, start_time, end_time,
                      duration_seconds, status, current_stage, rows_read, rows_written,
                      rows_rejected, bytes_processed, watermark, validation_result, error_details
               FROM migration_attempts WHERE run_id=?
               ORDER BY start_time DESC LIMIT ?""",
            [run_id, max(1, min(int(limit), 500))],
        ).fetchdf().to_dict("records"))

    def checkpoint_snapshot(self, run_id):
        def snapshot(connection):
            runs = connection.execute(
                "SELECT * FROM migration_runs WHERE run_id=?", [run_id]
            ).fetchdf().to_dict("records")
            attempts = connection.execute(
                "SELECT * FROM migration_attempts WHERE run_id=? ORDER BY start_time",
                [run_id],
            ).fetchdf().to_dict("records")
            return json.dumps(
                {"schema_version": 1, "run": runs, "attempts": attempts},
                default=str,
                separators=(",", ":"),
            ).encode("utf-8")
        return self._submit(snapshot, checkpoint=True)

    def close(self):
        if self._thread.is_alive():
            self._requests.put(None)
            self._thread.join(timeout=10)


_repository = None
_repository_lock = threading.Lock()


def get_metrics_repository():
    """Return the process-wide writer repository used by the application."""
    global _repository
    with _repository_lock:
        if _repository is None:
            _repository = MetricsRepository()
        return _repository
