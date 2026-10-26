"""Strict BigQuery GoogleSQL CREATE TABLE to Amazon Redshift DDL conversion."""
from dataclasses import asdict, dataclass, field
import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, SqlglotError


@dataclass
class DDLResult:
    sql: str = ""
    changes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    statement_count: int = 0

    def to_dict(self):
        return asdict(self)


@dataclass
class _Column:
    name: str
    target_type: str
    not_null: bool = False
    default: str = ""


@dataclass
class _Table:
    source_name: str
    target_schema: str
    target_table: str
    columns: list[_Column]
    if_not_exists: bool = False
    source_sortkeys: list[str] = field(default_factory=list)


_SIMPLE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,126}\Z")
_INTEGER_TYPES = {
    exp.DataType.Type.TINYINT, exp.DataType.Type.SMALLINT,
    exp.DataType.Type.INT, exp.DataType.Type.BIGINT,
}
_FLOAT_TYPES = {exp.DataType.Type.FLOAT, exp.DataType.Type.DOUBLE}


def _identifier(value, label="Identifier"):
    if not isinstance(value, str) or not _SIMPLE_IDENTIFIER.fullmatch(value):
        raise ValueError(f"{label} must start with a letter or underscore, contain only letters, numbers and underscores, and be at most 127 characters")
    return value.lower()


def _quote(value):
    return '"' + value.replace('"', '""') + '"'


def _parse_statements(sql, dialect):
    statements = []
    for node in sqlglot.parse(sql, read=dialect, error_level=ErrorLevel.RAISE):
        if node is not None and not isinstance(node, exp.Semicolon):
            statements.append(node)
    return statements


def validate_redshift_ddl(sql):
    """Parse-only validation. It does not connect to or execute against Redshift."""
    try:
        trees = _parse_statements(sql, "redshift")
        if not trees:
            raise ValueError("No DDL statements found")
        for tree in trees:
            if not isinstance(tree, exp.Create) or str(tree.args.get("kind", "")).upper() != "TABLE":
                raise ValueError("Only Redshift CREATE TABLE statements are allowed")
            if tree.args.get("replace") or tree.args.get("expression") is not None:
                raise ValueError("CREATE OR REPLACE and CTAS are outside the generated DDL subset")
            if not isinstance(tree.this, exp.Schema) or not tree.this.expressions:
                raise ValueError("CREATE TABLE must contain an explicit column schema")
        return {
            "valid": True,
            "message": "Redshift parser check passed. Object existence, permissions and engine execution have not been checked.",
        }
    except (SqlglotError, ValueError) as exc:
        return {"valid": False, "message": str(exc)}


class BigQueryDDLMigrator:
    """Convert the flat scalar DDL subset supported by the POC data pipeline."""

    def __init__(self, target_schema="public", table_mapping=None, string_length=65535,
                 diststyle="AUTO", distkey="", sortkey_mode="none", sortkeys=None):
        self.target_schema = _identifier(target_schema, "Target schema")
        self.string_length = int(string_length)
        if not 1 <= self.string_length <= 65535:
            raise ValueError("Default VARCHAR length must be between 1 and 65535")
        self.diststyle = str(diststyle).upper().strip()
        if self.diststyle not in {"AUTO", "EVEN", "ALL", "KEY"}:
            raise ValueError("Distribution style must be AUTO, EVEN, ALL or KEY")
        self.distkey = _identifier(distkey, "DISTKEY") if str(distkey).strip() else ""
        if self.diststyle == "KEY" and not self.distkey:
            raise ValueError("DISTSTYLE KEY requires a DISTKEY column")
        if self.diststyle != "KEY" and self.distkey:
            raise ValueError("A DISTKEY can only be used with DISTSTYLE KEY")
        self.sortkey_mode = str(sortkey_mode).lower().strip()
        if self.sortkey_mode not in {"none", "source", "manual"}:
            raise ValueError("Sort-key mode must be none, source or manual")
        self.sortkeys = [_identifier(v.strip(), "SORTKEY") for v in (sortkeys or []) if v.strip()]
        if len(self.sortkeys) != len(set(self.sortkeys)):
            raise ValueError("SORTKEY columns must be unique")
        if self.sortkey_mode == "manual" and not self.sortkeys:
            raise ValueError("Manual sort-key mode requires at least one SORTKEY column")
        if self.sortkey_mode != "manual" and self.sortkeys:
            raise ValueError("Manual SORTKEY columns require manual sort-key mode")
        self.table_mapping = {}
        if table_mapping is not None and not isinstance(table_mapping, dict):
            raise ValueError("Table mapping must be a JSON object")
        for source, target in (table_mapping or {}).items():
            if not isinstance(source, str) or not isinstance(target, str):
                raise ValueError("Table mappings must contain string keys and values")
            parts = target.split(".")
            if len(parts) != 2:
                raise ValueError("Each mapped target must be schema.table")
            self.table_mapping[source] = (
                _identifier(parts[0], "Mapped schema"),
                _identifier(parts[1], "Mapped table"),
            )

    def migrate(self, source):
        result = DDLResult()
        if not source or not source.strip():
            result.errors.append("Paste or upload a BigQuery CREATE TABLE statement first.")
            return result
        try:
            trees = _parse_statements(source, "bigquery")
        except SqlglotError as exc:
            result.errors.append(f"BigQuery parsing failed: {exc}")
            return result
        if not trees:
            result.errors.append("No DDL statements found.")
            return result
        result.statement_count = len(trees)
        tables = []
        claimed_targets = {}
        used_mappings = set()
        for index, tree in enumerate(trees, 1):
            try:
                table = self._table_from_ast(tree, result, used_mappings)
                target = (table.target_schema, table.target_table)
                previous = claimed_targets.get(target)
                if previous is not None and previous != table.source_name:
                    raise ValueError(
                        f"{previous} and {table.source_name} both map to {target[0]}.{target[1]}. "
                        "Add explicit table mappings to keep them distinct."
                    )
                claimed_targets[target] = table.source_name
                tables.append(table)
            except (SqlglotError, ValueError) as exc:
                result.errors.append(f"Statement {index}: {exc}")
        if result.errors:
            result.changes = list(dict.fromkeys(result.changes))
            result.warnings = list(dict.fromkeys(result.warnings))
            return result
        statements = []
        for table in tables:
            try:
                statements.append(self._render_table(table, result))
            except ValueError as exc:
                result.errors.append(f"Table {table.source_name}: {exc}")
        if result.errors:
            return result
        result.sql = "\n\n".join(statements)
        result.changes.insert(0, f"Converted {len(tables)} BigQuery CREATE TABLE statement(s) to Redshift DDL.")
        for key in self.table_mapping.keys() - used_mappings:
            result.warnings.append(f"Table mapping was not used: {key}")
        result.warnings.append(
            "Offline conversion only. Run the syntax check, review the physical design, and execute in a non-production Redshift schema first."
        )
        validation = validate_redshift_ddl(result.sql)
        if not validation["valid"]:
            result.errors.append(f"Generated Redshift DDL failed parser validation: {validation['message']}")
            result.sql = ""
        result.changes = list(dict.fromkeys(result.changes))
        result.warnings = list(dict.fromkeys(result.warnings))
        return result

    def migrate_metadata_table(self, project, dataset, table, columns,
                               partition_field=None, clustering_fields=None):
        """Convert one table returned by bigquery_connector.discover_tables."""
        result = DDLResult(statement_count=1)
        try:
            source_name = ".".join((str(project), str(dataset), str(table)))
            target_schema, target_table = self._target(source_name, str(table), set())
            specs = []
            seen = set()
            for raw in columns:
                name = _identifier(str(raw.get("Column", "")), "Column name")
                if name in seen:
                    raise ValueError(f"Duplicate target column after normalization: {name}")
                seen.add(name)
                mode = str(raw.get("Mode", "NULLABLE")).upper()
                if mode == "REPEATED":
                    raise ValueError(f"Column {name}: REPEATED fields require an explicit Redshift representation")
                if mode not in {"NULLABLE", "REQUIRED"}:
                    raise ValueError(f"Column {name}: unsupported BigQuery mode {mode}")
                target_type = self._metadata_type(raw, name, result)
                specs.append(_Column(name, target_type, mode == "REQUIRED"))
            if not specs:
                raise ValueError("The table has no columns")
            source_keys = []
            if partition_field:
                source_keys.append(_identifier(str(partition_field), "Partition column"))
            source_keys.extend(_identifier(str(v), "Clustering column") for v in (clustering_fields or []))
            source_keys = list(dict.fromkeys(source_keys))
            table_spec = _Table(source_name, target_schema, target_table, specs,
                                source_sortkeys=source_keys)
            result.sql = self._render_table(table_spec, result)
            result.changes.insert(0, f"Converted BigQuery metadata for {source_name} to Redshift DDL.")
            result.warnings.append(
                "The BigQuery API does not return a portable CREATE statement here; the DDL was generated from table schema metadata."
            )
            result.warnings.append(
                "Offline conversion only. Run the syntax check, review the physical design, and execute in a non-production Redshift schema first."
            )
            validation = validate_redshift_ddl(result.sql)
            if not validation["valid"]:
                raise ValueError(f"Generated Redshift DDL failed parser validation: {validation['message']}")
        except (TypeError, ValueError) as exc:
            result.errors.append(str(exc))
            result.sql = ""
        result.changes = list(dict.fromkeys(result.changes))
        result.warnings = list(dict.fromkeys(result.warnings))
        return result

    def _target(self, source_name, source_table, used_mappings):
        if source_name in self.table_mapping:
            used_mappings.add(source_name)
            return self.table_mapping[source_name]
        return self.target_schema, _identifier(source_table, "Target table")

    def _table_from_ast(self, tree, result, used_mappings):
        if not isinstance(tree, exp.Create) or str(tree.args.get("kind", "")).upper() != "TABLE":
            raise ValueError("Only CREATE TABLE statements are supported")
        if tree.args.get("replace"):
            raise ValueError("CREATE OR REPLACE TABLE has destructive replacement semantics. Choose a deployment strategy manually.")
        if tree.args.get("expression") is not None:
            raise ValueError("CREATE TABLE AS SELECT is data movement. Convert its SELECT in the Query Migrator and create the target table separately.")
        if not isinstance(tree.this, exp.Schema):
            raise ValueError("CREATE TABLE must contain an explicit column list; LIKE and COPY forms are unsupported")
        source_table = tree.this.this
        if not isinstance(source_table, exp.Table) or not source_table.name:
            raise ValueError("Could not read the BigQuery table name")
        source_name = ".".join(part.name for part in source_table.parts)
        target_schema, target_table = self._target(source_name, source_table.name, used_mappings)
        columns = []
        seen = set()
        for item in tree.this.expressions:
            if not isinstance(item, exp.ColumnDef):
                raise ValueError(f"Table constraint {type(item).__name__} needs manual migration and validation")
            name = _identifier(item.name, "Column name")
            if name in seen:
                raise ValueError(f"Duplicate target column after normalization: {name}")
            seen.add(name)
            target_type = self._ast_type(item.args.get("kind"), name, result)
            not_null = False
            default = ""
            for constraint in item.args.get("constraints") or []:
                kind = constraint.args.get("kind")
                if isinstance(kind, exp.NotNullColumnConstraint):
                    not_null = True
                elif isinstance(kind, exp.Properties):
                    result.warnings.append(f"Column {name}: BigQuery OPTIONS metadata was omitted from Redshift DDL.")
                elif isinstance(kind, exp.DefaultColumnConstraint):
                    default = self._default_value(kind.this, name)
                else:
                    raise ValueError(f"Column {name}: constraint {type(kind).__name__} needs manual migration")
            columns.append(_Column(name, target_type, not_null, default))
        if not columns:
            raise ValueError("The table has no columns")
        source_sortkeys = self._source_sortkeys(tree.args.get("properties"), result)
        result.changes.append(f"Table: {source_name} → {target_schema}.{target_table}")
        return _Table(source_name, target_schema, target_table, columns,
                      bool(tree.args.get("exists")), source_sortkeys)

    def _source_sortkeys(self, properties, result):
        keys = []
        if properties is None:
            return keys
        for prop in properties.expressions:
            if isinstance(prop, exp.TemporaryProperty):
                raise ValueError("Temporary tables are session-scoped and need a manual target design")
            if isinstance(prop, exp.PartitionedByProperty):
                candidates = []
                if isinstance(prop.this, exp.Identifier):
                    candidates = [prop.this.name]
                elif isinstance(prop.this, exp.Column):
                    candidates = [prop.this.name]
                else:
                    candidates = [column.name for column in prop.this.find_all(exp.Column)]
                if len(set(candidates)) == 1:
                    keys.append(_identifier(candidates[0], "Partition column"))
                else:
                    result.warnings.append("BigQuery partition expression could not be represented as a Redshift sort key.")
            elif isinstance(prop, exp.ClusterProperty):
                for expression in prop.expressions:
                    if not isinstance(expression, (exp.Column, exp.Identifier)):
                        raise ValueError("BigQuery CLUSTER BY expressions must be simple columns")
                    keys.append(_identifier(expression.name, "Clustering column"))
            elif isinstance(prop, exp.Property):
                result.warnings.append("BigQuery table OPTIONS metadata was omitted from Redshift DDL.")
            else:
                raise ValueError(f"BigQuery table property {type(prop).__name__} needs manual migration")
        return list(dict.fromkeys(keys))

    def _ast_type(self, dtype, column, result):
        if not isinstance(dtype, exp.DataType):
            raise ValueError(f"Column {column}: missing or unsupported data type")
        kind = dtype.this
        params = self._type_params(dtype, column)
        if dtype.args.get("nested") or kind in {
            exp.DataType.Type.ARRAY, exp.DataType.Type.STRUCT, exp.DataType.Type.JSON,
            exp.DataType.Type.GEOGRAPHY, exp.DataType.Type.BIGDECIMAL,
            exp.DataType.Type.INTERVAL, exp.DataType.Type.RANGE,
        }:
            raise ValueError(f"Column {column}: {kind.value} requires an explicit Redshift data representation")
        if kind in _INTEGER_TYPES:
            target = "BIGINT"
        elif kind in _FLOAT_TYPES:
            target = "DOUBLE PRECISION"
        elif kind == exp.DataType.Type.DECIMAL:
            target = self._decimal_type(params, column, result)
        elif kind == exp.DataType.Type.BOOLEAN:
            target = "BOOLEAN"
        elif kind == exp.DataType.Type.TEXT:
            length = params[0] if params else self.string_length
            if len(params) > 1 or not 1 <= length <= 65535:
                raise ValueError(f"Column {column}: STRING length must be between 1 and 65535 for Redshift VARCHAR")
            target = f"VARCHAR({length})"
        elif kind == exp.DataType.Type.DATE:
            target = "DATE"
        elif kind in {exp.DataType.Type.TIMESTAMP, exp.DataType.Type.TIMESTAMPTZ}:
            target = "TIMESTAMP"
            if kind == exp.DataType.Type.TIMESTAMPTZ:
                result.warnings.append("BigQuery TIMESTAMP columns become Redshift TIMESTAMP to match the POC Parquet COPY path; values are expected to be normalized to UTC.")
        else:
            raise ValueError(f"Column {column}: {kind.value} is outside the types supported by the BigQuery data migration POC")
        result.changes.append(f"Column {column}: BigQuery {dtype.sql(dialect='bigquery')} → Redshift {target}")
        return target

    def _metadata_type(self, raw, column, result):
        source = str(raw.get("Type", "")).upper()
        precision = raw.get("Precision")
        scale = raw.get("Scale")
        max_length = raw.get("Max Length")
        if source in {"INTEGER", "INT64"}:
            target = "BIGINT"
        elif source in {"FLOAT", "FLOAT64"}:
            target = "DOUBLE PRECISION"
        elif source in {"NUMERIC", "DECIMAL"}:
            params = [] if precision is None else [int(precision), int(scale or 0)]
            target = self._decimal_type(params, column, result)
        elif source in {"BOOLEAN", "BOOL"}:
            target = "BOOLEAN"
        elif source == "STRING":
            length = int(max_length) if max_length is not None else self.string_length
            if not 1 <= length <= 65535:
                raise ValueError(f"Column {column}: STRING length must be between 1 and 65535 for Redshift VARCHAR")
            target = f"VARCHAR({length})"
        elif source == "DATE":
            target = "DATE"
        elif source in {"DATETIME", "TIMESTAMP"}:
            target = "TIMESTAMP"
            if source == "TIMESTAMP":
                result.warnings.append("BigQuery TIMESTAMP columns become Redshift TIMESTAMP to match the POC Parquet COPY path; values are expected to be normalized to UTC.")
        else:
            raise ValueError(f"Column {column}: {source or 'unknown type'} is outside the types supported by the BigQuery data migration POC")
        result.changes.append(f"Column {column}: BigQuery {source} → Redshift {target}")
        return target

    @staticmethod
    def _type_params(dtype, column):
        params = []
        for item in dtype.expressions:
            value = item.this if isinstance(item, exp.DataTypeParam) else item
            if not isinstance(value, exp.Literal) or value.is_string:
                raise ValueError(f"Column {column}: data-type parameters must be integer literals")
            params.append(int(value.this))
        return params

    @staticmethod
    def _decimal_type(params, column, result):
        if not params:
            precision, scale = 38, 9
        elif len(params) == 1:
            precision, scale = params[0], 0
        elif len(params) == 2:
            precision, scale = params
        else:
            raise ValueError(f"Column {column}: NUMERIC accepts precision and optional scale")
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError(f"Column {column}: DECIMAL precision must be 1-38 and scale 0-precision")
        # google-cloud-bigquery materializes every NUMERIC as Arrow
        # decimal128(38, 9), including parameterized NUMERIC(p, s). Redshift
        # columnar COPY requires that physical Parquet decimal schema to match.
        if (precision, scale) != (38, 9):
            result.warnings.append(
                f"Column {column}: BigQuery NUMERIC({precision},{scale}) becomes DECIMAL(38,9) "
                "because the current BigQuery-to-Parquet client emits decimal128(38,9); the source parameter constraint is not retained."
            )
        return "DECIMAL(38,9)"

    @staticmethod
    def _default_value(value, column):
        if isinstance(value, (exp.Literal, exp.Boolean, exp.Null)):
            return value.sql(dialect="redshift")
        if isinstance(value, exp.Neg) and isinstance(value.this, exp.Literal) and not value.this.is_string:
            return value.sql(dialect="redshift")
        raise ValueError(f"Column {column}: only literal DEFAULT values are converted automatically")

    def _render_table(self, table, result):
        names = {column.name for column in table.columns}
        if self.distkey and self.distkey not in names:
            raise ValueError(f"DISTKEY column {self.distkey} does not exist")
        if self.sortkey_mode == "manual":
            sortkeys = self.sortkeys
        elif self.sortkey_mode == "source":
            sortkeys = table.source_sortkeys
            if not sortkeys:
                result.warnings.append(f"{table.source_name}: no usable BigQuery partition/clustering columns were found; no SORTKEY was emitted.")
        else:
            sortkeys = []
            if table.source_sortkeys:
                result.warnings.append(f"{table.source_name}: BigQuery partition/clustering settings were omitted; choose source-layout sort keys if you want them as a Redshift starting point.")
        missing = [key for key in sortkeys if key not in names]
        if missing:
            raise ValueError("SORTKEY columns do not exist: " + ", ".join(missing))
        column_lines = []
        for column in table.columns:
            line = f"  {_quote(column.name)} {column.target_type}"
            if column.default:
                line += f" DEFAULT {column.default}"
            if column.not_null:
                line += " NOT NULL"
            column_lines.append(line)
        create = "CREATE TABLE " + ("IF NOT EXISTS " if table.if_not_exists else "")
        lines = [f"{create}{_quote(table.target_schema)}.{_quote(table.target_table)} (",
                 ",\n".join(column_lines), ")", f"DISTSTYLE {self.diststyle}"]
        if self.diststyle == "KEY":
            lines.append(f"DISTKEY ({_quote(self.distkey)})")
            result.changes.append(f"Physical design: DISTSTYLE KEY on {self.distkey}.")
        else:
            result.changes.append(f"Physical design: DISTSTYLE {self.diststyle}.")
        if sortkeys:
            lines.append("COMPOUND SORTKEY (" + ", ".join(_quote(key) for key in sortkeys) + ")")
            result.changes.append("Physical design: COMPOUND SORTKEY on " + ", ".join(sortkeys) + ".")
            if self.sortkey_mode == "source":
                result.warnings.append("BigQuery partitioning/clustering was used only as an initial Redshift SORTKEY suggestion; verify it with Redshift workload data.")
        return "\n".join(lines) + ";"
