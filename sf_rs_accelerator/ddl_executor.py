"""Run converted Redshift DDL, with guard rails.

Only CREATE TABLE and CREATE SCHEMA statements are ever executed. Everything else
(DROP, TRUNCATE, DELETE, INSERT, GRANT, ALTER, CREATE TABLE ... AS SELECT, ...) is
blocked before any statement runs. A table that already exists is reported as
skipped, never replaced.

The module has no Streamlit or AWS dependency: callers pass in a ``runner``
callable that executes one SQL statement and raises on failure.
"""
import re

_ALLOWED = re.compile(r"^\s*CREATE\s+(TABLE|SCHEMA)\b", re.IGNORECASE)
_CTAS = re.compile(r"\bAS\s*\(?\s*(SELECT|WITH)\b", re.IGNORECASE)
_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]{0,126}$")
_LABEL = re.compile(
    r"^\s*CREATE\s+(TABLE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(;]+)",
    re.IGNORECASE,
)


def split_statements(sql: str) -> list:
    """Split SQL on semicolons that are outside quotes and comments.

    ``--`` and ``/* */`` comments are removed, so a statement never starts with
    a comment that could hide its real first keyword from the allow-list.
    """
    statements, buf = [], []
    i, n = 0, len(sql or "")
    quote = None  # "'" or '"' while inside a quoted section
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if quote:
            buf.append(ch)
            if ch == quote:
                if nxt == quote:  # doubled quote is an escaped quote
                    buf.append(nxt)
                    i += 1
                else:
                    quote = None
        elif ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif ch == "-" and nxt == "-":
            while i < n and sql[i] != "\n":
                i += 1
            continue
        elif ch == "/" and nxt == "*":
            end = sql.find("*/", i + 2)
            i = n if end == -1 else end + 2
            continue
        elif ch == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def statement_label(statement: str) -> str:
    """Short name for display, for example ``CREATE TABLE public.orders``."""
    drop = re.match(r"^\s*DROP\s+TABLE\s+IF\s+EXISTS\s+([^\s;()]+)", statement or "", re.IGNORECASE)
    if drop:
        return f"DROP TABLE {drop.group(1).replace(chr(34), '')}"
    match = _LABEL.match(statement or "")
    if match:
        return f"CREATE {match.group(1).upper()} {match.group(2).replace(chr(34), '')}"
    return " ".join((statement or "").split())[:60] or "(empty)"


def block_reason(statement: str):
    """Return why a statement may not run, or None when it is allowed."""
    if not _ALLOWED.match(statement or ""):
        return "only CREATE TABLE and CREATE SCHEMA statements can be executed"
    if _CTAS.search(statement):
        return "CREATE TABLE ... AS SELECT is not plain DDL"
    return None


_DROP = re.compile(r"^\s*DROP\s+TABLE\s+IF\s+EXISTS\s+([^\s;()]+)\s*$", re.IGNORECASE)
_CREATE_TABLE_NAME = re.compile(
    r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(;]+)", re.IGNORECASE)


def _norm(name: str) -> str:
    return name.replace('"', "").lower()


def is_replace_drop(statements, index: int) -> bool:
    """True for ``DROP TABLE IF EXISTS x`` directly followed by ``CREATE TABLE x``.

    This is the pair the Snowflake converter writes for ``CREATE OR REPLACE TABLE``.
    A DROP of anything else, or with CASCADE, is never a replace pair.
    """
    drop = _DROP.match(statements[index])
    if not drop or index + 1 >= len(statements):
        return False
    create = _CREATE_TABLE_NAME.match(statements[index + 1])
    return bool(create) and _norm(create.group(1)) == _norm(drop.group(1))


def prepare(sql: str, allow_replace: bool = False) -> dict:
    """Split ``sql`` and check every statement.

    A converter-generated replace pair (``DROP TABLE IF EXISTS x`` then
    ``CREATE TABLE x``) is handled according to ``allow_replace``:

    * ``False`` (default): the DROP is removed, so an existing table is left alone.
    * ``True``: the pair is kept and the table is dropped and recreated.

    Returns ``{"statements", "blocked", "replace", "drops_removed"}``. Nothing
    should be executed unless ``blocked`` is empty.
    """
    raw = split_statements(sql)
    statements, replace, removed = [], [], 0
    for i, statement in enumerate(raw):
        if is_replace_drop(raw, i):
            if allow_replace:
                statements.append(statement)
                replace.append(_norm(_DROP.match(statement).group(1)))
            else:
                removed += 1
            continue
        statements.append(statement)
    blocked = [(statement_label(s), block_reason(s)) for i, s in enumerate(statements)
               if block_reason(s) and not (allow_replace and is_replace_drop(statements, i))]
    return {"statements": statements, "blocked": blocked, "replace": replace,
            "drops_removed": removed}


def schemas_referenced(statements) -> list:
    """Schemas named by ``CREATE TABLE schema.table`` statements, excluding public."""
    found = []
    for statement in statements:
        match = _LABEL.match(statement or "")
        if not match or match.group(1).upper() != "TABLE":
            continue
        parts = [p.strip('"') for p in match.group(2).split(".")]
        if len(parts) >= 2:
            schema = parts[-2]
            if schema.lower() != "public" and schema not in found:
                found.append(schema)
    return found


def run(statements, runner, *, create_schemas=(), continue_on_error=False,
        allow_replace=False) -> list:
    """Execute ``statements`` one by one through ``runner(sql)``.

    Each name in ``create_schemas`` (``public`` is ignored) gets a
    ``CREATE SCHEMA IF NOT EXISTS`` first. Each result is
    ``{"statement", "status", "message"}`` where status is ``created``,
    ``dropped`` (replace mode only), ``skipped`` (already exists), ``failed``
    or ``not run``.

    ``allow_replace`` permits only a ``DROP TABLE IF EXISTS x`` directly followed
    by ``CREATE TABLE x``. It is not atomic: if the CREATE then fails, the old
    table is already gone.
    """
    plan = list(statements)
    prefix = []
    for name in create_schemas or ():
        name = (name or "").strip()
        if not name or name.lower() == "public":
            continue
        if not _SCHEMA_NAME.match(name):
            raise ValueError(f"Invalid schema name: {name!r}")
        prefix.append(f'CREATE SCHEMA IF NOT EXISTS "{name}"')
    plan = prefix + plan

    results, stopped, skip_create = [], False, None
    for index, statement in enumerate(plan):
        label = statement_label(statement)
        if stopped:
            results.append({"statement": label, "status": "not run",
                            "message": "an earlier statement failed"})
            continue
        is_drop = allow_replace and is_replace_drop(plan, index)
        reason = None if is_drop else block_reason(statement)
        if reason:  # defence in depth: run() is safe even if prepare() was skipped
            results.append({"statement": label, "status": "failed", "message": f"blocked: {reason}"})
            stopped = not continue_on_error
            continue
        if skip_create == index:  # the DROP before it failed, so do not create
            results.append({"statement": label, "status": "not run",
                            "message": "the DROP before it failed"})
            continue
        try:
            runner(statement)
        except Exception as exc:  # the runner raises on any Redshift error
            message = " ".join(str(exc).split())
            if not is_drop and "already exists" in message.lower():
                results.append({"statement": label, "status": "skipped",
                                "message": "already exists, left unchanged"})
            else:
                results.append({"statement": label, "status": "failed", "message": message[:400]})
                if is_drop:
                    skip_create = index + 1
                stopped = not continue_on_error
            continue
        if is_drop:
            results.append({"statement": label, "status": "dropped",
                            "message": "existing table removed"})
        else:
            results.append({"statement": label, "status": "created", "message": ""})
    return results


def summarize(results) -> dict:
    counts = {"created": 0, "dropped": 0, "skipped": 0, "failed": 0, "not run": 0}
    for item in results:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    return counts
