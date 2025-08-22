# sql_guard.py

import re
import logging
from typing import Tuple

# Set up logging
logging.basicConfig(
    filename='llm_errors.log',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger("SQLGuard")

class SQLGuard:
    def __init__(self, conn):
        self.conn = conn
        self.valid_columns = self._get_valid_columns()

    def _get_valid_columns(self) -> set:
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'SalesPlanTable'
        """)
        # Normalize to lowercase
        return {row[0].lower() for row in cur.fetchall()}

    def _fix_column_names(self, sql: str) -> str:
        """
        Fix common LLM hallucinations:
        - [ord_fy] → [OrderFY]
        - [amount] → [Amount]
        """
        fixes = {
            r"\[\s*mmmmyy\s*\]": "[monthyear]",
            r"\[\s*mmmyy\s*\]": "[monthyear]",
            r"\[\s*mmmy\s*\]": "[monthyear]",
            r"\[\s*mmyy\s*\]": "[monthyear]",
            r"\[\s*ord_fy\s*\]": "[OrderFY]",
            r"\[\s*ordfy\s*\]": "[OrderFY]",
            r"\[\s*fy\s*\]": "[OrderFY]",
            r"\[\s*amount\s*\]": "[Amount]",
            r"\[\s*value\s*\]": "[Amount]",
            r"\[\s*sales\s*\]": "[Amount]",
            r"\[\s*customer\s*\]": "[Customer_Name]",
            r"\[\s*cust\s*\]": "[Customer_Name]",
            r"\[\s*mfg\s*\]": "[MFGMode]",
            r"\[\s*mode\s*\]": "[MFGMode]",
            r"\[\s*type\s*\]": "[Type]",
            r"\[\s*doc\s*type\s*\]": "[Type]",
            r"\[\s*month\s*year\s*\]": "[monthyear]",
            r"\[\s*my\s*\]": "[monthyear]",
        }
        original = sql
        for pattern, replacement in fixes.items():
            sql = re.sub(pattern, replacement, sql, flags=re.IGNORECASE)
        if sql != original:
            logger.info(f"Fixed column names:\nOriginal: {original}\nFixed: {sql}")
        return sql

    def _fix_cast_fy(self, sql: str) -> str:
        """
        Fix CAST(OrderFY AS INT) → CAST(LEFT(OrderFY, 4) AS INT)
        """
        return re.sub(
            r"CAST\s*\(\s*\[?OrderFY\]?\s+AS\s+INT\s*\)",
            r"CAST(LEFT(OrderFY, 4) AS INT)",
            sql,
            flags=re.IGNORECASE
        )

    def repair_sql(self, sql: str) -> str:
        """
        Apply minimal, safe fixes.
        Order: brackets → columns → CAST → TOP
        """
        fixes = [
            self._fix_column_names,
            self._fix_cast_fy,
            self._fix_missing_group_by
        ]
        for fix in fixes:
            try:
                sql = fix(sql)
            except Exception as e:
                logger.warning(f"Error in {fix.__name__}: {e}")
        return sql.strip()

    def validate_sql(self, sql: str) -> bool:
        """
        Validate only real column references.
        Skip: keywords, strings, numbers, functions, aliases.
        Case-insensitive.
        """
        if not sql:
            return False

        # Remove comments
        sql_no_comment = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL)
        # Remove strings
        sql_clean = re.sub(r"'[^']*'", "", sql_no_comment)
        sql_clean = re.sub(r'"[^"]*"', "", sql_clean)
        sql_clean = re.sub(r"\b\d{2,}\b", "", sql_clean) # Remove numbers
        # Common SQL keywords
        SQL_KEYWORDS = {
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "HAVING", "AS",
            "DISTINCT", "ALL", "TOP", "OFFSET", "FETCH", "LIMIT", 
            "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN", "IS", "NULL", "ISNULL", "COALESCE", "CASE", "WHEN", "THEN", "ELSE", "END",
            "SUM", "COUNT", "AVG", "MIN", "MAX", "STDEV", "VAR", "GROUPING",
            "OVER", "PARTITION", "ORDER", "ROWS", "RANGE", "CURRENT ROW", "PRECEDING", "FOLLOWING",
            "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
            "CAST", "CONVERT", "TRY_CAST", "TRY_CONVERT", "DATEPART", "YEAR", "MONTH", "DAY", "DATENAME",
            "INT", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "DATE", "DATETIME", "BIT",
            "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "OUTER", "CROSS", "APPLY",
            "ON", "USING", "UNION", "UNION ALL", "EXCEPT", "INTERSECT",
            "WITH", "RECURSIVE", "EXISTS", "NOT EXISTS", "ANY", "SOME", "ALL",
            "PIVOT", "UNPIVOT", "FOR", "IN","DESC"
        }
        
        # Find all [col] or bare col
        tokens = re.finditer(r"\[\s*([^\]]+)\s*\]|\b([a-zA-Z_][\w]*)\b", sql_clean)
        invalid_columns = []

        for match in tokens:
            inner = (match.group(1) or match.group(2)).strip()
            if not inner:
                continue

            # Skip if it's a keyword
            if inner.upper() in SQL_KEYWORDS:
                continue

            # Skip if it's a table/schema
            if inner.lower() in {"dbo", "salesplantable"}:
                continue

            # Skip if it's an alias (after AS)
            if re.search(rf"\bAS\s+{re.escape(inner)}\b", sql, re.I):
                continue
            
            
            if re.search(rf"\bAS\s+\[\s*{re.escape(inner)}\s*\]", sql, re.I):
                continue

            # Skip if it's a number
            if re.match(r"^\d+$", inner):
                continue

            # Case-insensitive validation
            inner_lower = inner.lower()
            bracketed_lower = f"[{inner}]".lower()

            if (inner_lower not in self.valid_columns and 
                bracketed_lower not in self.valid_columns):
                invalid_columns.append(inner)

        # Block if any invalid columns found
        if invalid_columns:
            logger.warning(f"Invalid columns blocked: {invalid_columns} | SQL: {sql}")
            return False

        # Final: must be SELECT and safe
        cleaned = re.sub(r"\s+", " ", sql_no_comment).strip().lower()
        if not cleaned.lstrip().startswith("select"):
            return False

        write_keywords = ("insert", "update", "delete", "alter", "drop", "create", "merge", "exec", "truncate")
        if any(kw in cleaned for kw in write_keywords):
            return False

        return True
    
    def _fix_missing_group_by(self, sql: str) -> str:
        """
        Auto-add GROUP BY for non-aggregated columns when SELECT contains SUM, COUNT, etc.
        Only works for simple cases.
        """
        if not re.search(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", sql, re.I):
            return sql

        # Extract SELECT columns
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.I | re.DOTALL)
        if not select_match:
            return sql

        select_part = select_match.group(1).strip()
        # Remove aliases
        select_clean = re.sub(r"\s+AS\s+\w+", "", select_part, flags=re.I)

        # Extract column names: [col] or bare words
        col_matches = re.findall(r"\[\s*([^\]]+)\s*\]|\b([a-zA-Z_][\w]*)\b", select_clean)
        columns = [g1 or g2 for g1, g2 in col_matches if g1 or g2]

        # Find aggregated columns
        aggregated = []
        for col in columns:
            if re.search(rf"\b(SUM|COUNT|AVG|MIN|MAX)\s*\([^)]*{re.escape(col)}", sql, re.I):
                aggregated.append(col.lower())

        # Non-aggregated columns → need GROUP BY
        group_cols = [col for col in columns if col.lower() not in aggregated and col.upper() not in {
            "SUM", "COUNT", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END"
        }]

        if not group_cols:
            return sql

        # Remove existing GROUP BY
        sql_no_group = re.sub(r"\s+GROUP BY.*?(?:ORDER BY|HAVING|$)", " ", sql, flags=re.I | re.DOTALL)

        # Add GROUP BY
        group_by_clause = " GROUP BY " + ", ".join(f"[{col}]" if "[" in col or " " in col else col for col in group_cols)
        if "ORDER BY" in sql.upper():
            sql_no_group = re.sub(r"\s+(ORDER BY.*)", r" " + group_by_clause + r" \1", sql_no_group, flags=re.I)
        else:
            sql_no_group = sql_no_group.strip().rstrip(";") + group_by_clause

        return sql_no_group