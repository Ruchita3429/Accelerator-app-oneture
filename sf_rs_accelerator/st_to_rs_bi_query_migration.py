"""
Snowflake to Redshift BI Query Migration Tool
Uses SQLGlot to convert Snowflake-specific SQL to Redshift-compatible queries
"""

import sqlglot
from sqlglot import exp
from sqlglot.dialects import Snowflake, Redshift
import re
from typing import Optional


class BIQueryMigrator:
    """Converts Snowflake BI queries to Redshift compatible queries"""
    
    def __init__(self):
        # Configure SQLGlot for Snowflake input and Redshift output
        self.source_dialect = "snowflake"
        self.target_dialect = "redshift"
        
        # Snowflake-specific functions that need mapping
        self.snowflake_functions = {
            # String functions
            'STRING_AGG': 'LISTAGG',
            'REGEXP_CONTAINS': None,  # Needs custom implementation
            'REGEXP_EXTRACT': None,   # Needs custom implementation
            'REGEXP_REPLACE': 'REGEXP_REPLACE',  # Different syntax
            'NVL': 'COALESCE',
            'NVL2': None,  # Needs CASE expression
            
            # Date functions
            'DATE_TRUNC': 'DATE_TRUNC',
            'DATEDIFF': None,  # Redshift uses datediff differently
            'DATEADD': None,   # Redshift uses dateadd differently
            'TO_DATE': 'TO_DATE',
            'TO_TIMESTAMP': 'TO_TIMESTAMP',
            
            # Aggregate functions
            'ANY_VALUE': 'ANY_VALUE',
            'COUNTIF': None,  # Needs SUM(CASE WHEN...) 
            'ARRAY_AGG': 'ARRAY_AGG',
            
            # Window functions
            'ROW_NUMBER': 'ROW_NUMBER',
            'RANK': 'RANK',
            'DENSE_RANK': 'DENSE_RANK',
            'LAG': 'LAG',
            'LEAD': 'LEAD',
            'FIRST_VALUE': 'FIRST_VALUE',
            'LAST_VALUE': 'LAST_VALUE',
        }
    
    def migrate_query(self, query: str, preview: bool = True) -> str:
        """
        Main entry point to migrate a Snowflake query to Redshift
        
        Args:
            query: Snowflake SQL query string
            preview: If True, shows both original and converted query
            
        Returns:
            Converted Redshift-compatible query
        """
        if preview:
            print("=" * 60)
            print("ORIGINAL SNOWFLAKE QUERY:")
            print("=" * 60)
            print(query)
            print()
        
        # Step 1: Parse the Snowflake query
        try:
            parsed = sqlglot.parse_one(query, dialect=self.source_dialect)
        except Exception as e:
            print(f"Warning: Could not parse query with SQLGlot: {e}")
            print("Attempting manual conversion...")
            return self._manual_conversion(query)
        
        # Step 2: Convert to Redshift
        try:
            converted = parsed.sql(dialect=self.target_dialect)
        except Exception as e:
            print(f"Warning: SQLGlot conversion failed: {e}")
            return self._manual_conversion(query)
        
        # Step 3: Apply post-processing fixes
        converted = self._post_process(converted)
        
        if preview:
            print("=" * 60)
            print("CONVERTED REDSHIFT QUERY:")
            print("=" * 60)
            print(converted)
            print("=" * 60)
            
        
        return converted
    
    def _post_process(self, query: str) -> str:
        """Apply post-conversion fixes for known issues"""
        
        # Fix ARRAY_AGG - Redshift uses different syntax
        query = re.sub(
            r'ARRAY_AGG\s*\(\s*(\w+)\s*ORDER BY\s+(\w+)\s*\)',
            r'ARRAY_AGG(\1 ORDER BY \2)',
            query,
            flags=re.IGNORECASE
        )
        
        # Fix STRING_AGG -> LISTAGG
        query = re.sub(
            r'STRING_AGG\s*\(\s*(\w+)\s*,\s*[\'"](\w)[\'"]\s*ORDER BY\s+(\w+)\s*\)',
            r'LISTAGG(\1, \2) OVER (ORDER BY \3)',
            query,
            flags=re.IGNORECASE
        )
        
        # Fix COUNTIF -> SUM(CASE...)
        query = re.sub(
            r'COUNTIF\s*\(\s*(\w+)\s*\)',
            r'SUM(CASE WHEN \1 THEN 1 ELSE 0 END)',
            query,
            flags=re.IGNORECASE
        )
        
        # Fix NVL -> COALESCE (already handled by SQLGlot but ensure)
        query = re.sub(r'\bNVL\s*\(', r'COALESCE(', query, flags=re.IGNORECASE)
        
        # Fix REGEXP_CONTAINS -> similar pattern
        query = re.sub(
            r'REGEXP_CONTAINS\s*\(\s*(\w+)\s*,\s*[\'"]?([^,\'"]+)[\'"]?\s*\)',
            r'(REGEXP_like(\1, \2))',
            query,
            flags=re.IGNORECASE
        )
        
        # Fix DATE_TRUNC - ensure proper syntax
        query = re.sub(
            r"DATE_TRUNC\s*\(\s*'([^']+)'\s*,\s*(\w+)\s*\)",
            r"DATE_TRUNC('\1', \2)",
            query,
            flags=re.IGNORECASE
        )
        
        # Fix QUALIFY clause (not supported in Redshift)
        if 'QUALIFY' in query.upper():
            print("Warning: QUALIFY clause is not supported in Redshift")
            print("You will need to manually handle this with a subquery")
        
        # Fix DISTINCT ON (Redshift specific)
        query = re.sub(
            r'DISTINCT\s+ON\s*\(([^)]+)\)',
            r'DISTINCT ON (\1)',
            query,
            flags=re.IGNORECASE
        )
        
        return query
    
    def _manual_conversion(self, query: str) -> str:
        """Fallback manual conversion for complex queries"""
        
        # Basic replacements
        converted = query
        
        # Replace common patterns
        replacements = [
            (r'\bNVL\s*\(', 'COALESCE('),
            (r'\bGETDATE\(\)', 'CURRENT_TIMESTAMP'),
            (r'\bSYSDATE\b', 'CURRENT_DATE'),
            (r'\bCURRENT_TIMESTAMP\b', 'GETDATE()'),
            (r'\bDATEADD\s*\(\s*day\s*,\s*(\d+)\s*,\s*', r"DATE_ADD('day', \1, "),
            (r'\bDATEADD\s*\(\s*month\s*,\s*(\d+)\s*,\s*', r"DATE_ADD('month', \1, "),
            (r'\bDATEDIFF\s*\(\s*day\s*,\s*', r"(DATE_PART('day', "),
            (r'\bTO_VARCHAR\s*\(', 'CAST('),
            (r'\bTRY_CAST\s*\(', 'CAST('),
        ]
        
        for pattern, replacement in replacements:
            converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
        
        return converted
    
    def migrate_file(self, input_file: str, output_file: str) -> None:
        """Migrate all queries from a file"""
        
        print(f"Reading queries from: {input_file}")
        
        with open(input_file, 'r') as f:
            queries = f.read().split(';')
        
        converted_queries = []
        
        for i, query in enumerate(queries):
            query = query.strip()
            if query and not query.startswith('--'):
                print(f"\n--- Query {i+1} ---")
                converted = self._migrate_query(query)
                converted_queries.append(converted)
        
        # Write output
        with open(output_file, 'w') as f:
            f.write('\n\n'.join(converted_queries))
        
        print(f"\nConverted {len(converted_queries)} queries")
        print(f"Output written to: {output_file}")


def convert_snowflake_to_redshift(query: str) -> str:
    """
    Simple function to convert a single Snowflake query to Redshift
    
    Usage:
        query = "SELECT * FROM table WHERE NVL(col, 'default') = 'value'"
        redshift_query = convert_snowflake_to_redshift(query)
    """
    migrator = BIQueryMigrator()
    return migrator.migrate_query(query, preview=False)


# ============================================================
# STORED PROCEDURE MIGRATOR
# ============================================================

class StoredProcedureMigrator:
    """Converts Snowflake stored procedures to Redshift compatible procedures"""
    
    def __init__(self):
        self.query_migrator = BIQueryMigrator()
        
        # Snowflake to Redshift function mappings for procedures
        self.function_mappings = {
            # System functions
            'CURRENT_TIMESTAMP()': 'GETDATE()',
            'CURRENT_DATE()': 'CURRENT_DATE()',
            'CURRENT_TIME()': 'CURRENT_TIME()',
            'CURRENT_USER()': 'CURRENT_USER()',
            'SESSION_ID()': 'SESSION_USER()',
            
            # String functions
            'CONCAT_WS': 'CONCAT_WS',
            'CONCAT': 'CONCAT',
            'LENGTH': 'LEN',
            'SUBSTR': 'SUBSTRING',
            'INSTR': 'CHARINDEX',
            'UPPER': 'UPPER',
            'LOWER': 'LOWER',
            'TRIM': 'TRIM',
            'LTRIM': 'LTRIM',
            'RTRIM': 'RTRIM',
            
            # Numeric functions
            'ROUND': 'ROUND',
            'CEIL': 'CEILING',
            'FLOOR': 'FLOOR',
            'ABS': 'ABS',
            'MOD': '%',  # Redshift uses modulo operator
            'POWER': 'POWER',
            'SQRT': 'SQRT',
            
            # Date functions
            'DATEDIFF': 'DATEDIFF',
            'DATEADD': 'DATEADD',
            'DATE_TRUNC': 'DATE_TRUNC',
        }
    
    def is_stored_procedure(self, code: str) -> bool:
        """Check if code is a stored procedure"""
        return bool(re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE', code, re.IGNORECASE))
    
    def migrate_procedure(self, procedure: str, preview: bool = True) -> str:
        """
        Convert Snowflake stored procedure to Redshift compatible procedure
        
        Args:
            procedure: Snowflake stored procedure definition
            preview: If True, shows both original and converted procedure
            
        Returns:
            Converted Redshift-compatible procedure
        """
        if preview:
            print("=" * 80)
            print("ORIGINAL SNOWFLAKE STORED PROCEDURE:")
            print("=" * 80)
            print(procedure)
            print()
        
        converted = self._convert_procedure(procedure)
        
        if preview:
            print("=" * 80)
            print("CONVERTED REDSHIFT STORED PROCEDURE:")
            print("=" * 80)
            print(converted)
            print("=" * 80)
        
        return converted
    
    def _convert_procedure(self, procedure: str) -> str:
        """Convert the procedure body"""
        converted = procedure
        
        # Step 1: Convert CREATE PROCEDURE syntax
        converted = self._convert_procedure_signature(converted)
        
        # Step 2: Convert variable declarations
        converted = self._convert_variable_declarations(converted)
        
        # Step 3: Convert control flow statements
        converted = self._convert_control_flow(converted)
        
        # Step 4: Convert SQL statements within procedure
        converted = self._convert_embedded_queries(converted)
        
        # Step 5: Convert function calls
        converted = self._convert_functions(converted)
        
        # Step 6: Convert error handling
        converted = self._convert_error_handling(converted)
        
        # Step 7: Convert EXECUTE IMMEDIATE
        converted = self._convert_execute_immediate(converted)
        
        # Step 8: Add compatibility comments
        converted = self._add_compatibility_warnings(procedure, converted)
        
        return converted
    
    def _convert_procedure_signature(self, procedure: str) -> str:
        """Convert CREATE PROCEDURE signature from Snowflake to Redshift"""
        
        # Match CREATE [OR REPLACE] PROCEDURE pattern
        pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(\w+)\s*\((.*?)\)\s+(?:RETURNS\s+\w+)?\s*(?:NOT\s+NULL)?\s*(?:LANGUAGE\s+\w+)?\s*(?:CALLED\s+ON\s+NULL\s+INPUT)?\s*(?:IMMUTABLE|STABLE|VOLATILE)?\s*AS'
        
        match = re.search(pattern, procedure, re.IGNORECASE | re.DOTALL)
        if match:
            proc_name = match.group(1)
            params = match.group(2)
            
            # Convert parameters
            converted_params = self._convert_parameters(params)
            
            # Redshift uses CREATE PROCEDURE ... AS $$...$$
            replacement = f'CREATE OR REPLACE PROCEDURE {proc_name}({converted_params})\nAS $$'
            converted = re.sub(pattern, replacement, procedure, flags=re.IGNORECASE | re.DOTALL)
        else:
            converted = procedure
        
        # Replace END with END PROCEDURE and add $$ terminator
        if '$$' not in converted and 'LANGUAGE' not in converted.upper():
            converted = re.sub(r'\bEND\s*;?\s*$', 'END PROCEDURE\n$$;', converted, flags=re.IGNORECASE | re.MULTILINE)
        
        return converted
    
    def _convert_parameters(self, params: str) -> str:
        """Convert procedure parameters"""
        if not params.strip():
            return params
        
        # Split parameters
        param_list = [p.strip() for p in params.split(',')]
        converted_params = []
        
        for param in param_list:
            # Snowflake: IN variable_name type DEFAULT value
            # Redshift: variable_name type DEFAULT value
            param = re.sub(r'^IN\s+', '', param, flags=re.IGNORECASE)
            param = re.sub(r'^OUT\s+', '', param, flags=re.IGNORECASE)
            param = re.sub(r'^INOUT\s+', '', param, flags=re.IGNORECASE)
            converted_params.append(param)
        
        return ', '.join(converted_params)
    
    def _convert_variable_declarations(self, procedure: str) -> str:
        """Convert DECLARE statements to Redshift format"""
        
        # Snowflake: DECLARE variable_name data_type [DEFAULT value];
        # Redshift: variable_name data_type [DEFAULT value];
        
        # Convert DECLARE statements
        procedure = re.sub(
            r'DECLARE\s+(\w+)\s+(\w+(?:\s*\(\s*\d+\s*\))?)\s*(?:DEFAULT\s+(.+?))?;',
            r'\1 \2 := \3;',
            procedure,
            flags=re.IGNORECASE | re.MULTILINE
        )
        
        # Convert data types
        procedure = procedure.replace('NUMBER', 'NUMERIC')
        procedure = procedure.replace('VARCHAR2', 'VARCHAR')
        procedure = procedure.replace('INT', 'INTEGER')
        procedure = procedure.replace('BOOLEAN', 'BOOLEAN')
        
        return procedure
    
    def _convert_control_flow(self, procedure: str) -> str:
        """Convert control flow statements (IF, WHILE, FOR, etc.)"""
        
        # IF THEN ELSE END IF -> IF THEN ELSE END IF (same in both)
        # But Snowflake uses THEN while Redshift might require slightly different format
        
        # Convert FOR loops
        # Snowflake: FOR variable IN (query) DO ... END FOR
        # Redshift: FOR variable IN SELECT ... LOOP ... END LOOP
        procedure = re.sub(
            r'FOR\s+(\w+)\s+IN\s*\((.*?)\)\s+DO',
            r'FOR \1 IN \2 LOOP',
            procedure,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        # Convert END FOR to END LOOP
        procedure = re.sub(r'\bEND\s+FOR\b', 'END LOOP', procedure, flags=re.IGNORECASE)
        
        # Convert WHILE loops (already compatible)
        # Ensure END WHILE is converted to END LOOP or END WHILE
        
        return procedure
    
    def _convert_embedded_queries(self, procedure: str) -> str:
        """Convert SQL queries embedded in procedures"""
        
        # Extract all queries and convert them
        # Find SELECT, INSERT, UPDATE, DELETE statements
        query_pattern = r'(SELECT|INSERT|UPDATE|DELETE)[\s\S]*?(?=;|\bEND\b|\bELSE\b)'
        
        def convert_query(match):
            query = match.group(0)
            # Use the query migrator
            return self.query_migrator._manual_conversion(query)
        
        procedure = re.sub(query_pattern, convert_query, procedure, flags=re.IGNORECASE)
        
        return procedure
    
    def _convert_functions(self, procedure: str) -> str:
        """Convert function calls"""
        
        for snowflake_func, redshift_func in self.function_mappings.items():
            if snowflake_func in procedure.upper():
                procedure = re.sub(
                    f'\\b{snowflake_func}\\b',
                    redshift_func,
                    procedure,
                    flags=re.IGNORECASE
                )
        
        # Special function conversions
        # NVL -> COALESCE
        procedure = re.sub(r'\bNVL\s*\(', 'COALESCE(', procedure, flags=re.IGNORECASE)
        
        # COUNTIF -> SUM(CASE...)
        procedure = re.sub(
            r'COUNTIF\s*\(\s*(\w+)\s*\)',
            r'SUM(CASE WHEN \1 THEN 1 ELSE 0 END)',
            procedure,
            flags=re.IGNORECASE
        )
        
        return procedure
    
    def _convert_error_handling(self, procedure: str) -> str:
        """Convert error handling from Snowflake to Redshift"""
        
        # Snowflake: EXCEPTION WHEN ... THEN
        # Redshift: EXCEPTION WHEN ... THEN (similar)
        
        # Convert DECLARE condition
        procedure = re.sub(
            r'DECLARE\s+\w+\s+CONDITION',
            '-- Redshift does not support named conditions, use SQLSTATE',
            procedure,
            flags=re.IGNORECASE
        )
        
        # Convert exception handler format
        procedure = re.sub(
            r'EXCEPTION\s+WHEN\s+statement_error',
            'EXCEPTION WHEN SQLEXCEPTION',
            procedure,
            flags=re.IGNORECASE
        )
        
        return procedure
    
    def _convert_execute_immediate(self, procedure: str) -> str:
        """Convert EXECUTE IMMEDIATE to Redshift format"""
        
        # Snowflake: EXECUTE IMMEDIATE query_string [USING ...]
        # Redshift: EXECUTE query_string [USING ...]
        
        procedure = re.sub(
            r'EXECUTE\s+IMMEDIATE\s+',
            'EXECUTE ',
            procedure,
            flags=re.IGNORECASE
        )
        
        return procedure
    
    def _add_compatibility_warnings(self, original: str, converted: str) -> str:
        """Add comments about compatibility issues and unsupported features"""
        
        warnings = []
        
        # Check for unsupported features
        if 'TASK' in original.upper():
            warnings.append('-- ⚠️  WARNING: Snowflake TASK is not supported in Redshift. Consider using external scheduling.')
        
        if 'STREAM' in original.upper():
            warnings.append('-- ⚠️  WARNING: Snowflake STREAM change data capture not supported in Redshift.')
        
        if 'CALL' in original.upper() and 'PROCEDURE' in original.upper():
            warnings.append('-- ⚠️  NOTE: Stored procedure calls in Redshift require different syntax.')
        
        if 'TRY' in original.upper() or 'CATCH' in original.upper():
            warnings.append('-- ⚠️  WARNING: TRY-CATCH blocks require conversion to Redshift EXCEPTION handling.')
        
        if 'LATERAL' in original.upper():
            warnings.append('-- ⚠️  WARNING: Snowflake LATERAL keyword not directly supported in Redshift.')
        
        if warnings:
            # Add warnings at the beginning
            warning_section = '\n'.join(warnings) + '\n\n'
            converted = warning_section + converted
        
        return converted
    
    def migrate_procedure_file(self, input_file: str, output_file: str) -> None:
        """Migrate all stored procedures from a file"""
        
        print(f"Reading stored procedures from: {input_file}")
        
        with open(input_file, 'r') as f:
            content = f.read()
        
        # Split procedures by END; pattern
        procedures = re.split(r'(?=CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE)', content)
        
        converted_procedures = []
        
        for i, procedure in enumerate(procedures):
            procedure = procedure.strip()
            if procedure and 'CREATE' in procedure.upper():
                print(f"\n--- Procedure {i+1} ---")
                converted = self.migrate_procedure(procedure, preview=False)
                converted_procedures.append(converted)
        
        # Write output
        with open(output_file, 'w') as f:
            f.write('\n\n'.join(converted_procedures))
        
        print(f"\nConverted {len(converted_procedures)} procedures")
        print(f"Output written to: {output_file}")


# ============================================================
# STORED PROCEDURE MIGRATOR
# ============================================================

class StoredProcedureMigrator:
    """Converts Snowflake stored procedures to Redshift compatible procedures"""
    
    def __init__(self):
        self.query_migrator = BIQueryMigrator()
        
        # Snowflake to Redshift function mappings for procedures
        self.function_mappings = {
            # System functions
            'CURRENT_TIMESTAMP()': 'GETDATE()',
            'CURRENT_DATE()': 'CURRENT_DATE()',
            'CURRENT_TIME()': 'CURRENT_TIME()',
            'CURRENT_USER()': 'CURRENT_USER()',
            'SESSION_ID()': 'SESSION_USER()',
            
            # String functions
            'CONCAT_WS': 'CONCAT_WS',
            'CONCAT': 'CONCAT',
            'LENGTH': 'LEN',
            'SUBSTR': 'SUBSTRING',
            'INSTR': 'CHARINDEX',
            'UPPER': 'UPPER',
            'LOWER': 'LOWER',
            'TRIM': 'TRIM',
            'LTRIM': 'LTRIM',
            'RTRIM': 'RTRIM',
            
            # Numeric functions
            'ROUND': 'ROUND',
            'CEIL': 'CEILING',
            'FLOOR': 'FLOOR',
            'ABS': 'ABS',
            'MOD': '%',  # Redshift uses modulo operator
            'POWER': 'POWER',
            'SQRT': 'SQRT',
            
            # Date functions
            'DATEDIFF': 'DATEDIFF',
            'DATEADD': 'DATEADD',
            'DATE_TRUNC': 'DATE_TRUNC',
        }
    
    def is_stored_procedure(self, code: str) -> bool:
        """Check if code is a stored procedure"""
        return bool(re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE', code, re.IGNORECASE))
    
    def migrate_procedure(self, procedure: str, preview: bool = True) -> str:
        """
        Convert Snowflake stored procedure to Redshift compatible procedure
        
        Args:
            procedure: Snowflake stored procedure definition
            preview: If True, shows both original and converted procedure
            
        Returns:
            Converted Redshift-compatible procedure
        """
        if preview:
            print("=" * 80)
            print("ORIGINAL SNOWFLAKE STORED PROCEDURE:")
            print("=" * 80)
            print(procedure)
            print()
        
        converted = self._convert_procedure(procedure)
        
        if preview:
            print("=" * 80)
            print("CONVERTED REDSHIFT STORED PROCEDURE:")
            print("=" * 80)
            print(converted)
            print("=" * 80)
        
        return converted
    
    def _convert_procedure(self, procedure: str) -> str:
        """Convert the procedure body"""
        converted = procedure
        
        # Step 1: Convert CREATE PROCEDURE syntax
        converted = self._convert_procedure_signature(converted)
        
        # Step 2: Convert variable declarations
        converted = self._convert_variable_declarations(converted)
        
        # Step 3: Convert control flow statements
        converted = self._convert_control_flow(converted)
        
        # Step 4: Convert SQL statements within procedure
        converted = self._convert_embedded_queries(converted)
        
        # Step 5: Convert function calls
        converted = self._convert_functions(converted)

        converted = self._convert_return_handling(converted)
        
        # Step 6: Convert error handling
        converted = self._convert_error_handling(converted)
        
        # Step 7: Convert EXECUTE IMMEDIATE
        converted = self._convert_execute_immediate(converted)
        
        # Step 8: Add compatibility comments
        converted = self._add_compatibility_warnings(procedure, converted)
        
        return converted
    
    def _convert_procedure_signature(self, procedure: str) -> str:

        pattern = re.compile(
            r'''
            CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+
            (\w+)\s*
            \((.*?)\)\s*
            RETURNS\s+TABLE
            .*?
            \$\$
            ''',
            re.IGNORECASE | re.DOTALL | re.VERBOSE
        )

        match = pattern.search(procedure)

        if not match:
            return procedure

        proc_name = match.group(1)
        params = match.group(2).strip()

        converted_params = self._convert_parameters(params)

        if converted_params:
            converted_params += ", p_result INOUT refcursor"
        else:
            converted_params = "p_result INOUT refcursor"

        replacement = f"""CREATE OR REPLACE PROCEDURE {proc_name}(
        {converted_params}
    )
    AS $$"""

        converted = pattern.sub(
            replacement,
            procedure,
            count=1
        )

        converted = re.sub(
            r'\$\$\s*LANGUAGE\s+SQL\s*;',
            '$$ LANGUAGE plpgsql;',
            converted,
            flags=re.IGNORECASE
        )

        return converted
    
    def _convert_parameters(self, params: str) -> str:
        """Convert procedure parameters"""
        if not params.strip():
            return params
        
        # Split parameters
        param_list = [p.strip() for p in params.split(',')]
        converted_params = []
        
        for param in param_list:
            # Snowflake: IN variable_name type DEFAULT value
            # Redshift: variable_name type DEFAULT value
            param = re.sub(r'^IN\s+', '', param, flags=re.IGNORECASE)
            param = re.sub(r'^OUT\s+', '', param, flags=re.IGNORECASE)
            param = re.sub(r'^INOUT\s+', '', param, flags=re.IGNORECASE)
            converted_params.append(param)
        
        return ', '.join(converted_params)
    
    def _convert_variable_declarations(self, procedure: str) -> str:

        datatype_mappings = {
            'NUMBER': 'NUMERIC',
            'VARCHAR2': 'VARCHAR',
            'STRING': 'VARCHAR(MAX)',
            'TEXT': 'VARCHAR(MAX)',
            'TIMESTAMP_NTZ': 'TIMESTAMP',
            'TIMESTAMP_LTZ': 'TIMESTAMP',
            'TIMESTAMP_TZ': 'TIMESTAMPTZ',
            'VARIANT': 'SUPER',
            'OBJECT': 'SUPER',
            'ARRAY': 'SUPER'
        }

        for sf_type, rs_type in datatype_mappings.items():

            procedure = re.sub(
                rf'\b{re.escape(sf_type)}\b',
                rs_type,
                procedure,
                flags=re.IGNORECASE
            )

        procedure = re.sub(
            r'\bINT\b',
            'INTEGER',
            procedure,
            flags=re.IGNORECASE
        )

        procedure = re.sub(
            r'CURRENT_DATE\s*\(\s*\)',
            'CURRENT_DATE',
            procedure,
            flags=re.IGNORECASE
        )

        procedure = re.sub(
            r'GETDATE\(\)\(\)',
            'GETDATE()',
            procedure,
            flags=re.IGNORECASE
        )

        return procedure
    
    def _convert_control_flow(self, procedure: str) -> str:
        """Convert control flow statements (IF, WHILE, FOR, etc.)"""
        
        # IF THEN ELSE END IF -> IF THEN ELSE END IF (same in both)
        # But Snowflake uses THEN while Redshift might require slightly different format
        
        # Convert FOR loops
        # Snowflake: FOR variable IN (query) DO ... END FOR
        # Redshift: FOR variable IN SELECT ... LOOP ... END LOOP
        procedure = re.sub(
            r'FOR\s+(\w+)\s+IN\s*\((.*?)\)\s+DO',
            r'FOR \1 IN \2 LOOP',
            procedure,
            flags=re.IGNORECASE | re.DOTALL
        )
        
        # Convert END FOR to END LOOP
        procedure = re.sub(r'\bEND\s+FOR\b', 'END LOOP', procedure, flags=re.IGNORECASE)
        
        # Convert WHILE loops (already compatible)
        # Ensure END WHILE is converted to END LOOP or END WHILE
        
        return procedure
    
    def _convert_embedded_queries(self, procedure: str) -> str:
        """Convert SQL queries embedded in procedures"""
        
        # Extract all queries and convert them
        # Find SELECT, INSERT, UPDATE, DELETE statements
        query_pattern = r'(SELECT|INSERT|UPDATE|DELETE)[\s\S]*?(?=;|\bEND\b|\bELSE\b)'
        
        def convert_query(match):
            query = match.group(0)
            # Use the query migrator
            return self.query_migrator._manual_conversion(query)
        
        procedure = re.sub(query_pattern, convert_query, procedure, flags=re.IGNORECASE)
        
        return procedure
    
    def _convert_functions(self, procedure: str) -> str:
        """Convert function calls"""
        
        for snowflake_func, redshift_func in self.function_mappings.items():
            if snowflake_func in procedure.upper():
                procedure = re.sub(
                    f'\\b{snowflake_func}\\b',
                    redshift_func,
                    procedure,
                    flags=re.IGNORECASE
                )
        
        # Special function conversions
        # NVL -> COALESCE
        procedure = re.sub(r'\bNVL\s*\(', 'COALESCE(', procedure, flags=re.IGNORECASE)
        
        # COUNTIF -> SUM(CASE...)
        procedure = re.sub(
            r'COUNTIF\s*\(\s*(\w+)\s*\)',
            r'SUM(CASE WHEN \1 THEN 1 ELSE 0 END)',
            procedure,
            flags=re.IGNORECASE
        )
        
        return procedure
    
    def _convert_error_handling(self, procedure: str) -> str:
        """Convert error handling from Snowflake to Redshift"""
        
        # Snowflake: EXCEPTION WHEN ... THEN
        # Redshift: EXCEPTION WHEN ... THEN (similar)
        
        # Convert DECLARE condition
        procedure = re.sub(
            r'DECLARE\s+\w+\s+CONDITION',
            '-- Redshift does not support named conditions, use SQLSTATE',
            procedure,
            flags=re.IGNORECASE
        )
        
        # Convert exception handler format
        procedure = re.sub(
            r'EXCEPTION\s+WHEN\s+statement_error',
            'EXCEPTION WHEN SQLEXCEPTION',
            procedure,
            flags=re.IGNORECASE
        )
        
        return procedure
    
    def _convert_execute_immediate(self, procedure: str) -> str:
        """Convert EXECUTE IMMEDIATE to Redshift format"""
        
        # Snowflake: EXECUTE IMMEDIATE query_string [USING ...]
        # Redshift: EXECUTE query_string [USING ...]
        
        procedure = re.sub(
            r'EXECUTE\s+IMMEDIATE\s+',
            'EXECUTE ',
            procedure,
            flags=re.IGNORECASE
        )
        
        return procedure
    
    def _add_compatibility_warnings(self, original: str, converted: str) -> str:
        """Add comments about compatibility issues and unsupported features"""
        
        warnings = []
        
        # Check for unsupported features
        if 'TASK' in original.upper():
            warnings.append('-- ⚠️  WARNING: Snowflake TASK is not supported in Redshift. Consider using external scheduling.')
        
        if 'STREAM' in original.upper():
            warnings.append('-- ⚠️  WARNING: Snowflake STREAM change data capture not supported in Redshift.')
        
        if 'CALL' in original.upper() and 'PROCEDURE' in original.upper():
            warnings.append('-- ⚠️  NOTE: Stored procedure calls in Redshift require different syntax.')
        
        if 'TRY' in original.upper() or 'CATCH' in original.upper():
            warnings.append('-- ⚠️  WARNING: TRY-CATCH blocks require conversion to Redshift EXCEPTION handling.')
        
        if 'LATERAL' in original.upper():
            warnings.append('-- ⚠️  WARNING: Snowflake LATERAL keyword not directly supported in Redshift.')
        
        if warnings:
            # Add warnings at the beginning
            warning_section = '\n'.join(warnings) + '\n\n'
            converted = warning_section + converted
        
        return converted
    
    def migrate_procedure_file(self, input_file: str, output_file: str) -> None:
        """Migrate all stored procedures from a file"""
        
        print(f"Reading stored procedures from: {input_file}")
        
        with open(input_file, 'r') as f:
            content = f.read()
        
        # Split procedures by END; pattern
        procedures = re.split(r'(?=CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE)', content)
        
        converted_procedures = []
        
        for i, procedure in enumerate(procedures):
            procedure = procedure.strip()
            if procedure and 'CREATE' in procedure.upper():
                print(f"\n--- Procedure {i+1} ---")
                converted = self.migrate_procedure(procedure, preview=False)
                converted_procedures.append(converted)
        
        # Write output
        with open(output_file, 'w') as f:
            f.write('\n\n'.join(converted_procedures))
        
        print(f"\nConverted {len(converted_procedures)} procedures")
        print(f"Output written to: {output_file}")

    def _convert_return_handling(self, procedure: str) -> str:

        procedure = re.sub(
            r'(\w+)\s*:=\s*ROW_COUNT\s*;',
            r'GET DIAGNOSTICS \1 := ROW_COUNT;',
            procedure,
            flags=re.IGNORECASE
        )

        select_pattern = re.compile(
            r'SELECT\s+(.*?)\s*;',
            re.IGNORECASE | re.DOTALL
        )

        matches = list(select_pattern.finditer(procedure))

        if matches:

            last_match = matches[-1]

            replacement = (
                f'OPEN p_result FOR '
                f'SELECT {last_match.group(1)};'
            )

            procedure = (
                procedure[:last_match.start()]
                + replacement
                + procedure[last_match.end():]
            )

        return procedure


# ============================================================
# EXAMPLE USAGE
# ============================================================

if __name__ == "__main__":
    
    # Example Snowflake BI queries
    example_queries = [
        # Example 1: Basic query with NVL
        """
        SELECT 
            customer_id,
            NVL(region, 'Unknown') as region,
            COUNT(*) as order_count,
            SUM(amount) as total_amount
        FROM orders
        WHERE order_date >= DATEADD(month, -3, CURRENT_DATE())
        GROUP BY customer_id, region
        """,
        
        # Example 2: Window function with ROW_NUMBER
        """
        SELECT 
            department,
            employee_name,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
        FROM employees
        """,
        
        # Example 3: String aggregation
        """
        SELECT 
            department,
            STRING_AGG(employee_name, ',' ORDER BY salary DESC) as employees
        FROM employees
        GROUP BY department
        """,
        
        # Example 4: Date functions
        """
        SELECT 
            order_date,
            DATE_TRUNC('month', order_date) as month_start,
            DATEDIFF(day, order_date, CURRENT_DATE()) as days_ago
        FROM orders
        """,
        
        # Example 5: Conditional aggregation
        """
        SELECT 
            department,
            COUNTIF(status = 'active') as active_count,
            COUNTIF(status = 'inactive') as inactive_count
        FROM employees
        """,
    ]
    
    # Example Snowflake stored procedures
    example_procedures = [
        # Example 1: Simple stored procedure with variables and queries
        """
        CREATE OR REPLACE PROCEDURE sp_update_sales(p_region VARCHAR)
        RETURNS TABLE AS
        $$
        DECLARE
            v_updated_rows INTEGER DEFAULT 0;
            v_current_date DATE := CURRENT_DATE();
        BEGIN
            UPDATE sales_summary
            SET updated_date = v_current_date
            WHERE region = p_region;
            
            v_updated_rows := ROW_COUNT;
            
            SELECT v_updated_rows AS rows_updated;
        END;
        $$
        LANGUAGE SQL;
        """,
        
        # Example 2: Procedure with IF-THEN-ELSE
        """
        CREATE OR REPLACE PROCEDURE sp_process_orders(p_order_date DATE)
        RETURNS VARCHAR AS
        $$
        DECLARE
            v_order_count INTEGER;
            v_status VARCHAR(50);
        BEGIN
            SELECT COUNT(*) INTO v_order_count
            FROM orders
            WHERE order_date = p_order_date;
            
            IF v_order_count > 100 THEN
                v_status := 'HIGH_VOLUME';
                UPDATE daily_stats
                SET volume_status = v_status
                WHERE process_date = p_order_date;
            ELSIF v_order_count > 50 THEN
                v_status := 'MEDIUM_VOLUME';
            ELSE
                v_status := 'LOW_VOLUME';
            END IF;
            
            RETURN v_status;
        END;
        $$
        LANGUAGE SQL;
        """,
        
        # Example 3: Procedure with loop
        """
        CREATE OR REPLACE PROCEDURE sp_archive_old_records(p_days_old INTEGER)
        RETURNS VARCHAR AS
        $$
        DECLARE
            v_archived_count INTEGER := 0;
            v_batch_size INTEGER := 1000;
            v_rec RECORD;
        BEGIN
            FOR v_rec IN
                SELECT id, created_date
                FROM raw_events
                WHERE created_date < CURRENT_DATE() - p_days_old
                LIMIT v_batch_size
            LOOP
                INSERT INTO archived_events
                SELECT * FROM raw_events WHERE id = v_rec.id;
                
                DELETE FROM raw_events WHERE id = v_rec.id;
                v_archived_count := v_archived_count + 1;
            END LOOP;
            
            RETURN 'Archived ' || v_archived_count || ' records';
        END;
        $$
        LANGUAGE SQL;
        """,
    ]
    
    # Run migration examples
    print("\n" + "=" * 80)
    print("SNOWFLAKE TO REDSHIFT QUERY MIGRATION")
    print("=" * 80)
    
    migrator = BIQueryMigrator()
    
    for i, query in enumerate(example_queries, 1):
        print(f"\n\n{'#' * 80}")
        print(f"# QUERY EXAMPLE {i}")
        print(f"{'#' * 80}")
        
        converted = migrator.migrate_query(query)
        
        print(f"\n✓ Query Example {i} converted successfully")
    
    # Run stored procedure migration examples
    print("\n\n" + "=" * 80)
    print("SNOWFLAKE TO REDSHIFT STORED PROCEDURE MIGRATION")
    print("=" * 80)
    
    proc_migrator = StoredProcedureMigrator()
    
    for i, procedure in enumerate(example_procedures, 1):
        print(f"\n\n{'#' * 80}")
        print(f"# PROCEDURE EXAMPLE {i}")
        print(f"{'#' * 80}")
        
        converted_proc = proc_migrator.migrate_procedure(procedure)
        
        print(f"\n✓ Procedure Example {i} converted successfully")