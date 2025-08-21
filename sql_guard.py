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
        Final safety check: only allow safe, valid SQL.
        """
        if not sql:
            return False

        # Remove comments
        cleaned = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()

        # Must be SELECT
        if not cleaned.upper().lstrip().startswith("SELECT"):
            logger.warning(f"Blocked non-SELECT: {sql}")
            return False

        # Block write operations
        write_keywords = ("insert", "update", "delete", "alter", "drop", "create", "merge", "exec", "truncate")
        if any(kw in cleaned.lower() for kw in write_keywords):
            logger.warning(f"Blocked write operation: {sql}")
            return False

        # Extract column references and validate
        # Skip: keywords, strings, numbers
        no_strings = re.sub(r"'[^']*'", "", sql)
        no_dbl_strings = re.sub(r'"[^"]*"', "", no_strings)
        tokens = re.finditer(r"\[\s*([^\]]+)\s*\]|\b([a-zA-Z_][\w]*)\b", no_dbl_strings)

        for match in tokens:
            inner = match.group(1) or match.group(2)
            inner_clean = inner.strip()

            # Skip SQL keywords
            if inner_clean.upper() in {
                "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "TOP",
                "SUM", "COUNT", "AVG", "MIN", "MAX", "CAST", "INT", "AS",
                "AND", "OR", "IN", "LIKE", "IS", "NULL", "NOT", "UNION",
                "CASE", "WHEN", "THEN", "ELSE", "END", "OVER", "PARTITION",
                "JOIN", "ON", "USING", "WITH", "INTO", "WHEN", "THEN", "OVER"
            }:
                continue

            # Skip table/schema
            if inner_clean.lower() in {"dbo", "salesplantable"}:
                continue

            # Skip if after AS (alias)
            if re.search(rf"\bAS\s+{re.escape(inner_clean)}\b", sql, re.I):
                continue

            # Validate column
            if (inner_clean not in self.valid_columns and 
                f"[{inner_clean}]" not in self.valid_columns and
                inner_clean != "*"):
                logger.warning(f"Invalid column detected: {inner_clean} in SQL: {sql}")
                return False

        return True