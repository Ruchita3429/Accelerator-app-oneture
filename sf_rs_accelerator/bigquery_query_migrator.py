"""Offline BigQuery GoogleSQL SELECT migration to Amazon Redshift for the POC."""
from dataclasses import asdict, dataclass, field
import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel, SqlglotError
from sqlglot.optimizer.scope import traverse_scope


@dataclass
class MigrationResult:
    sql: str = ""
    changes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    statement_count: int = 0

    def to_dict(self):
        return asdict(self)


# Functions outside this reviewed POC subset are rejected, even if SQLGlot
# happens to emit a same-named function that Redshift does not implement.
SUPPORTED_FUNCTIONS = set("""
Coalesce If Case Count CountIf Sum Avg Min Max Abs Ceil Floor Round Sqrt Pow
Exp Ln Log Lower Upper Trim Length Concat Substring Replace Nullif
RowNumber Rank DenseRank PercentRank CumeDist Ntile Lag Lead FirstValue LastValue
Cast TryCast CurrentDate CurrentTimestamp Date DateAdd DateSub DateDiff DateTrunc
TimestampAdd TimestampSub TimestampDiff Extract GroupConcat RegexpLike TimeToStr
TsOrDsToDate SafeDivide Greatest Least Exists
""".split())

BLOCKED_NODES = set("""
Unnest Array ArrayAgg ArrayConcat ArraySize Struct Bracket Dot Parameter Placeholder
Pivot Unpivot Lateral TableSample MatchRecognize JSONExtract JSONExtractScalar
Insert Update Delete Create Drop Merge Command Transaction Commit Rollback Declare Into
""".split())


def _target_identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,126}", value):
        raise ValueError("Target names must be simple SQL identifiers, at most 127 characters")
    return value.lower()


def _parse_statements(sql, dialect):
    statements, pending_comments = [], []
    for node in sqlglot.parse(sql, read=dialect, error_level=ErrorLevel.RAISE):
        if isinstance(node, exp.Semicolon):
            pending_comments.extend(node.comments or [])
        elif node is not None:
            node.add_comments(pending_comments)
            pending_comments = []
            statements.append(node)
    if pending_comments and statements:
        statements[-1].add_comments(pending_comments)
    return statements


def validate_redshift_syntax(sql):
    """Parser validation only: no connection, object lookup or execution."""
    try:
        trees = _parse_statements(sql, 'redshift')
        if not trees:
            raise ValueError('No SQL statements found')
        for tree in trees:
            if not isinstance(tree, (exp.Select, exp.SetOperation)):
                raise ValueError('Only SELECT queries and set operations are supported')
            if any(not select.expressions for select in tree.find_all(exp.Select)):
                raise ValueError('A SELECT statement has no output expressions')
        return {'valid': True, 'message': 'Redshift parser check passed. Table existence, types and result equivalence have not been checked.'}
    except (SqlglotError, ValueError) as exc:
        return {'valid': False, 'message': str(exc)}


class BigQueryQueryMigrator:
    def __init__(self, target_schema='public', table_mapping=None):
        self.target_schema = _target_identifier(target_schema)
        self.table_mapping = {}
        if table_mapping is not None and not isinstance(table_mapping, dict):
            raise ValueError('Table mapping must be a JSON object')
        for source, target in (table_mapping or {}).items():
            if not isinstance(source, str) or not isinstance(target, str):
                raise ValueError('Table mappings must contain string keys and values')
            parts = target.split('.')
            if len(parts) != 2:
                raise ValueError('Each mapped target must be schema.table')
            self.table_mapping[source] = tuple(_target_identifier(p) for p in parts)

    def migrate(self, source):
        result = MigrationResult()
        if not source.strip():
            result.errors.append('Paste or upload a BigQuery SELECT query first.')
            return result
        try:
            trees = _parse_statements(source, 'bigquery')
        except SqlglotError as exc:
            result.errors.append(f'BigQuery parsing failed: {exc}')
            return result
        if not trees:
            result.errors.append('No SQL statements found.')
            return result
        result.statement_count = len(trees)
        converted, claimed_targets, used_mappings = [], {}, set()
        for index, tree in enumerate(trees, 1):
            try:
                self._check_supported(tree, result.warnings)
                self._map_tables(tree, claimed_targets, used_mappings, result.changes)
                tree = self._rewrite(tree, result.changes, result.warnings)
                sql = tree.sql(dialect='redshift', pretty=True, unsupported_level=ErrorLevel.RAISE)
                validation = validate_redshift_syntax(sql)
                if not validation['valid']:
                    raise ValueError(validation['message'])
                converted.append(sql.rstrip(';') + ';')
            except (SqlglotError, ValueError) as exc:
                result.errors.append(f'Statement {index}: {exc}')
        if not result.errors:
            result.sql = '\n\n'.join(converted)
            result.changes.insert(0, f'Converted {len(trees)} BigQuery SELECT statement(s) to Redshift SQL.')
            for key in self.table_mapping.keys() - used_mappings:
                result.warnings.append(f'Table mapping was not used: {key}')
            result.warnings.append('Offline conversion only. Verify target columns/types and compare results on representative data before using the SQL.')
        # Never publish a partial SQL file when any statement is unsupported.
        result.changes = list(dict.fromkeys(result.changes))
        result.warnings = list(dict.fromkeys(result.warnings))
        return result

    def migrate_query(self, query, preview=False):
        result = self.migrate(query)
        if result.errors:
            raise ValueError('\n'.join(result.errors))
        return result.sql

    @staticmethod
    def _check_supported(tree, warnings):
        if not isinstance(tree, (exp.Select, exp.SetOperation)):
            raise ValueError('Only SELECT queries, CTEs and set operations are supported. DDL, DML, scripts and procedures need separate migration.')
        for node in tree.walk():
            name = type(node).__name__
            if name in BLOCKED_NODES:
                raise ValueError(f'{name} needs manual migration (nested data, parameters or unsupported relational syntax).')
            if isinstance(node, exp.Func) and name not in SUPPORTED_FUNCTIONS:
                label = node.name if isinstance(node, exp.Anonymous) else name
                raise ValueError(f'Function {label} is outside the supported BigQuery-to-Redshift subset; no automatic equivalent is assumed.')
            if isinstance(node, exp.Func) and (node.args.get('zone') or node.args.get('timezone') or (isinstance(node, exp.CurrentDate) and node.this is not None)):
                raise ValueError('Explicit timezone arguments require manual conversion; they cannot be replaced by the UTC assumption.')
            if isinstance(node, exp.Cast) and node.args.get('format'):
                raise ValueError('CAST with FORMAT requires manual conversion of the formatting rules.')
            if isinstance(node, exp.Select):
                if not node.expressions or node.args.get('kind') or node.args.get('into'):
                    raise ValueError('SELECT must have output expressions; SELECT AS STRUCT/VALUE and SELECT INTO are unsupported.')
            if isinstance(node, exp.Star) and any(node.args.get(k) for k in ('except_', 'replace', 'rename')):
                raise ValueError('SELECT * EXCEPT/REPLACE requires schema-aware expansion. List the required columns explicitly.')
            if isinstance(node, exp.Table):
                if not isinstance(node.this, exp.Identifier) or any(c in node.name for c in ('*', '$', '@')) or node.args.get('version'):
                    raise ValueError('Wildcard tables, table decorators, time travel and table-valued functions need manual mapping.')
                if node.db.upper() == 'INFORMATION_SCHEMA' or node.name.upper().startswith('_PARTITION'):
                    raise ValueError('BigQuery metadata and partition pseudo-columns do not map to Redshift business tables.')
            if isinstance(node, exp.Column) and node.name.upper() in ('_TABLE_SUFFIX', '_PARTITIONTIME', '_PARTITIONDATE'):
                raise ValueError('BigQuery table/partition pseudo-columns need an explicit target-column design.')
            if isinstance(node, exp.DataType) and node.this in (exp.DataType.Type.BIGDECIMAL, exp.DataType.Type.ARRAY, exp.DataType.Type.STRUCT, exp.DataType.Type.JSON, exp.DataType.Type.GEOGRAPHY):
                raise ValueError(f'{node.this.value} requires an explicit Redshift data representation.')
            if isinstance(node, exp.CountIf) and isinstance(node.this, exp.Distinct):
                raise ValueError('COUNTIF(DISTINCT ...) needs manual review.')
            if name in ('DateAdd', 'DateSub', 'DateDiff', 'DateTrunc'):
                unit = node.args.get('unit')
                if unit and unit.name.upper() not in ('DAY', 'MONTH', 'QUARTER', 'YEAR'):
                    raise ValueError('Calendar WEEK/ISO/custom date units require manual review of boundary rules.')
            if name in ('TimestampAdd', 'TimestampSub', 'TimestampDiff'):
                unit = node.args.get('unit')
                if unit is None or unit.name.upper() not in ('MICROSECOND', 'MILLISECOND', 'SECOND', 'MINUTE', 'HOUR', 'DAY'):
                    raise ValueError('Only fixed-duration timestamp units are supported.')
            if isinstance(node, exp.SetOperation) and node.args.get('by_name'):
                raise ValueError('Set operations BY NAME require explicit matching column lists.')
            if isinstance(node, exp.With) and node.args.get('recursive'):
                raise ValueError('Recursive CTEs require manual review of Redshift recursion rules.')
            if isinstance(node, exp.Window):
                spec = node.args.get('spec')
                if spec and str(spec.args.get('kind', '')).upper() == 'RANGE':
                    raise ValueError('RANGE window frames require manual migration; replacing RANGE with ROWS can change results for ties.')
                if node.args.get('order') and not spec and isinstance(node.this, (exp.AggFunc, exp.FirstValue, exp.LastValue)):
                    raise ValueError('Ordered aggregate/value windows need an explicit compatible ROWS frame. BigQuery default RANGE semantics cannot be silently replaced.')

    def _map_tables(self, tree, claimed_targets, used_mappings, changes):
        for scope in traverse_scope(tree):
            for column in scope.columns:
                if column.table:
                    owner = scope
                    while owner is not None and column.table.casefold() not in {name.casefold() for name in owner.sources}:
                        owner = owner.parent
                    if owner is None:
                        raise ValueError(f'Qualifier {column.table} is not a table/CTE alias in scope. Nested fields require manual migration.')
            # scopes distinguish physical tables from CTE references.
            for alias, (_, source) in scope.selected_sources.items():
                if not isinstance(source, exp.Table):
                    continue
                original = '.'.join(p.name for p in source.parts)
                if original in self.table_mapping:
                    target = self.table_mapping[original]
                    used_mappings.add(original)
                else:
                    target = (self.target_schema, _target_identifier(source.name))
                previous = claimed_targets.get(target)
                if previous is not None and previous != original:
                    raise ValueError(f'{previous} and {original} both map to {target[0]}.{target[1]}. Add explicit table mappings to keep them distinct.')
                claimed_targets[target] = original
                alias_ident = source.args['alias'].this.copy() if source.args.get('alias') else source.this.copy()
                alias_ident.set('quoted', True)
                for column in scope.columns:
                    if column.table and column.table.casefold() == alias.casefold():
                        column.set('table', alias_ident.copy())
                        column.set('db', None)
                        column.set('catalog', None)
                source.set('catalog', None)
                source.set('db', exp.to_identifier(target[0], quoted=True))
                source.set('this', exp.to_identifier(target[1], quoted=True))
                source.set('alias', exp.TableAlias(this=alias_ident))
                changes.append(f'Table: {original} → {target[0]}.{target[1]}')

    @staticmethod
    def _rewrite(tree, changes, warnings):
        # Snapshot source nodes so transformations do not reprocess new nodes.
        for node in reversed(list(tree.walk())):
            name = type(node).__name__
            if isinstance(node, exp.CountIf):
                node.replace(exp.Count(this=exp.Case(ifs=[exp.If(this=node.this.copy(), true=exp.Literal.number(1))])))
                changes.append('COUNTIF → COUNT(CASE WHEN ... THEN 1 END), preserving zero for an empty input.')
            elif isinstance(node, (exp.Greatest, exp.Least)):
                values = [node.this, *node.expressions]
                missing = exp.or_(*(exp.Is(this=v.copy(), expression=exp.Null()) for v in values))
                node.replace(exp.Case(ifs=[exp.If(this=missing, true=exp.Null())], default=node.copy()))
                changes.append('GREATEST/LEAST: preserve BigQuery NULL propagation with an explicit CASE.')
            elif isinstance(node, exp.TimestampDiff):
                unit = node.args['unit'].name.upper()
                divisor = {'MICROSECOND': 1, 'MILLISECOND': 1000, 'SECOND': 1000000,
                           'MINUTE': 60000000, 'HOUR': 3600000000, 'DAY': 86400000000}[unit]
                delta = exp.Anonymous(this='DATEDIFF', expressions=[exp.Var(this='MICROSECOND'), node.expression.copy(), node.this.copy()])
                denominator = exp.Cast(this=exp.Literal.number(divisor), to=exp.DataType.build('BIGINT'))
                replacement = delta if divisor == 1 else exp.Div(this=delta, expression=denominator, typed=True)
                node.replace(replacement)
                changes.append('TIMESTAMP_DIFF → elapsed microseconds divided into whole units, avoiding boundary-count differences.')
            elif isinstance(node, (exp.TimestampAdd, exp.TimestampSub)):
                amount = node.expression.copy()
                if isinstance(node, exp.TimestampSub):
                    amount = exp.Neg(this=amount)
                node.replace(exp.Anonymous(this='DATEADD', expressions=[node.args['unit'].copy(), amount, node.this.copy()]))
                changes.append('Timestamp interval arithmetic → DATEADD.')
            elif isinstance(node, (exp.DateAdd, exp.DateSub, exp.DateTrunc)):
                node.replace(exp.Cast(this=node.copy(), to=exp.DataType.build('DATE')))
                changes.append('Preserved DATE return types for date arithmetic/truncation with an explicit DATE cast.')
            elif isinstance(node, exp.DataType):
                if node.this == exp.DataType.Type.TEXT:
                    node.replace(exp.DataType.build('VARCHAR(65535)', dialect='redshift'))
                    warnings.append('STRING casts use VARCHAR(65535); Redshift has a finite string-size limit.')
                elif node.this == exp.DataType.Type.DECIMAL and not node.expressions:
                    node.replace(exp.DataType.build('DECIMAL(38,9)', dialect='redshift'))
                    changes.append('Unparameterized BigQuery NUMERIC casts → DECIMAL(38,9).')
            if isinstance(node, exp.TryCast):
                warnings.append('SAFE_CAST → TRY_CAST: Redshift rejects blank strings and some source/target type pairs. Review input types and empty-string handling.')
            if isinstance(node, (exp.SafeDivide, exp.Div)):
                warnings.append('Division may use DOUBLE PRECISION to preserve fractional results. Review NUMERIC precision, zero denominators and overflow behavior.')
            if name in ('GroupConcat', 'RegexpLike', 'TimeToStr'):
                warnings.append(f'{name}: review ordering, size limits, pattern/format semantics and representative edge cases on Redshift.')
            if name == 'Length':
                warnings.append('LENGTH: review trailing spaces and STRING/BYTES semantics on the target columns.')
            if name.startswith(('Date', 'Timestamp', 'Current')) or name in ('Extract', 'TsOrDsToDate'):
                warnings.append('Date/time conversion assumes the migrated timestamp columns and Redshift session use UTC; verify types, timezone and calendar edge cases.')
        # Redshift requires a relation alias when QUALIFY immediately follows FROM.
        for select in tree.find_all(exp.Select):
            if select.args.get('qualify') and select.args.get('from_'):
                relation = select.args['from_'].this
                if isinstance(relation, exp.Table) and not relation.alias:
                    relation.set('alias', exp.TableAlias(this=relation.this.copy()))
                elif isinstance(relation, exp.Subquery) and not relation.alias:
                    relation.set('alias', exp.TableAlias(this=exp.to_identifier('_bq_qualify_source', quoted=True)))
        return tree
