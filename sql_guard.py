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
        """Fetch real column names as a set for fast lookup."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'SalesPlanTable'
        """)
        return {row[0] for row in cur.fetchall()}

    def _fix_column_names(self, sql: str) -> str:
        """
        Fix common LLM hallucinations:
        - [MMMYY] → [MMMMYY]
        - [ord_fy] → [OrderFY]
        - [amount] → [Amount]
        """
        fixes = {
            r"\[\s*mmmmyy\s*\]": "[MMMMYY]",
            r"\[\s*mmmyy\s*\]": "[MMMMYY]",
            r"\[\s*mmmy\s*\]": "[MMMMYY]",
            r"\[\s*mmyy\s*\]": "[MMMMYY]",
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
        """Fix unsafe CAST(OrderFY AS INT) → safe LEFT(OrderFY, 4)"""
        if "CAST" in sql.upper() and "OrderFY" in sql:
            return re.sub(
                r"CAST\s*\(\s*[^)]*?OrderFY[^)]*?AS\s+INT\s*\)",
                r"CAST(LEFT(OrderFY, 4) AS INT)",
                sql,
                flags=re.IGNORECASE
            )
        return sql

    def _fix_top_placement(self, sql: str) -> str:
        """Ensure TOP 100 is right after SELECT."""
        if "TOP" not in sql.upper():
            return re.sub(r"^SELECT\b", "SELECT TOP 100", sql, flags=re.IGNORECASE)
        return sql

    def repair_sql(self, sql: str) -> str:
        """
        Apply minimal, safe fixes.
        Order: brackets → columns → CAST → TOP
        """
        fixes = [
            self._fix_column_names,
            self._fix_cast_fy,
            self._fix_top_placement,
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
        """
        if not sql:
            return False

        # Remove comments
        sql_no_comment = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL)
        # Remove strings
        sql_clean = re.sub(r"'[^']*'", "", sql_no_comment)
        sql_clean = re.sub(r'"[^"]*"', "", sql_clean)

        # Common SQL keywords (add more if needed)
        SQL_KEYWORDS = {
            "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "TOP", "AS",
            "SUM", "COUNT", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END",
            "AND", "OR", "IN", "LIKE", "IS", "NULL", "NOT", "UNION", "OVER", "PARTITION",
            "JOIN", "ON", "USING", "WITH", "INTO", "CAST", "INT", "VARCHAR", "DATE",
            "OVER", "PARTITION", "RANK", "DENSE_RANK", "ROW_NUMBER"
        }

        # Find all [col] or bare col, but skip keywords and function calls
        tokens = re.finditer(r"\[\s*([^\]]+)\s*\]|\b([a-zA-Z_][\w]*)\b", sql_clean)
        invalid_columns = []

        for match in tokens:
            inner = (match.group(1) or match.group(2)).strip()

            # Skip if it's a keyword
            if inner.upper() in SQL_KEYWORDS:
                continue

            # Skip if it's a table/schema
            if inner.lower() in {"dbo", "salesplantable", "salesplantable"}:
                continue

            # Skip if it's an alias (after AS)
            if re.search(rf"\bAS\s+{re.escape(inner)}\b", sql, re.I):
                continue

            # Skip if it's a function argument (e.g., SUM(col), CAST(col AS INT))
            if re.search(rf"\b(CAST|SUM|COUNT|AVG|MIN|MAX|ROUND)\s*\(\s*{re.escape(inner)}", sql, re.I):
                continue

            # Skip if it's a number or looks like one
            if re.match(r"^\d+$", inner):
                continue

            # Validate column
            if (inner not in self.valid_columns and 
                f"[{inner}]" not in self.valid_columns):
                invalid_columns.append(inner)

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