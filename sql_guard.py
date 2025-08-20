# sql_guard.py

import re
import logging
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='llm_errors.log',
    level=logging.WARNING,
    format='%(asctime)s | %(levelname)s | %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger("SQLGuard")

class SQLGuard:
    def __init__(self, conn):
        self.conn = conn
        self.valid_columns = self._get_valid_columns()
        self.column_alias_map = self._build_column_map()

    def _get_valid_columns(self) -> List[str]:
        """Fetch all real column names from the SalesPlanTable."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'SalesPlanTable'
        """)
        return [row[0] for row in cur.fetchall()]

    def _build_column_map(self) -> Dict[str, str]:
        """
        Map common LLM hallucinations to real column names.
        Keys are lowercase base names.
        """
        return {
            # MMMYY variants
            "mmmmyy": "MMMMYY",
            "mmmyy": "MMMMYY",
            "mmmy": "MMMMYY",
            "mmyy": "MMMMYY",
            # OrderFY
            "ord_fy": "OrderFY",
            "orderfy": "OrderFY",
            "fy": "OrderFY",
            "ordfy": "OrderFY",
            # monthyear
            "monthyear": "monthyear",
            "month_year": "monthyear",
            "my": "monthyear",
            # MFGMode
            "mfg": "MFGMode",
            "mfgmode": "MFGMode",
            "manufacturingmode": "MFGMode",
            # Type
            "doctype": "Type",
            "ordertype": "Type",
            # Amount
            "amt": "Amount",
            "value": "Amount",
            # Date
            "orderdate": "OrderDate",
        }

    def resolve_column(self, col: str) -> str:
        """Resolve a column name (with typo) to the correct one."""
        clean = re.sub(r"[\[\]\s]", "", col).lower()
        return self.column_alias_map.get(clean, col)

    def fix_column_names(self, sql: str) -> str:
        """
        Fix common column name errors in SQL.
        Handles: [mmmmyy], mmmmyy, [MMMYY], etc.
        """
        original = sql
        changed = False

        # Find all [col] or bare col references
        tokens = re.finditer(r"\[\s*([^\]]+)\s*\]|\b([a-zA-Z_][\w]*)\b", sql)
        for match in reversed(list(tokens)):
            token = match.group(0)
            inner = match.group(1) or match.group(2)

            # Skip SQL keywords
            if inner.upper() in {
                "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "TOP",
                "SUM", "COUNT", "AVG", "MIN", "MAX", "CAST", "INT", "AS",
                "AND", "OR", "IN", "LIKE", "IS", "NULL", "NOT", "UNION"
            }:
                continue

            resolved = self.resolve_column(inner)
            if resolved != inner:
                # Replace in original
                start, end = match.span()
                sql = sql[:start] + f"[{resolved}]" + sql[end:]
                changed = True

        if changed:
            logger.warning(f"Fixed column names in SQL:\nOriginal: {original}\nFixed: {sql}")
        return sql

    def validate_columns(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Check if SQL uses only valid columns.
        Returns (is_valid, invalid_columns)
        """
        invalid = []
        tokens = re.finditer(r"\[\s*([^\]]+)\s*\]|\b([a-zA-Z_][\w]*)\b", sql)
        for match in tokens:
            inner = match.group(1) or match.group(2)
            if inner.upper() in {
                "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "TOP",
                "SUM", "COUNT", "AVG", "MIN", "MAX", "CAST", "INT", "AS",
                "AND", "OR", "IN", "LIKE", "IS", "NULL", "NOT", "UNION"
            }:
                continue
            if inner not in self.valid_columns and f"[{inner}]" not in self.valid_columns:
                resolved = self.resolve_column(inner)
                if resolved not in self.valid_columns:
                    invalid.append(inner)

        return len(invalid) == 0, invalid

    def fix_top_placement(self, sql: str) -> str:
        """Ensure TOP 100 is right after SELECT."""
        if "TOP" in sql.upper():
            sql = re.sub(r"\s+TOP\s+\d+", "", sql, flags=re.IGNORECASE)
        return re.sub(r"^SELECT\b", "SELECT TOP 100", sql, flags=re.IGNORECASE)

    def fix_cast_fy(self, sql: str) -> str:
        """Fix CAST(OrderFY AS INT) → LEFT(OrderFY, 4) for '2024-25' format."""
        if "CAST" in sql.upper() and "OrderFY" in sql:
            return re.sub(
                r"CAST\s*\(\s*[^)]*?OrderFY[^)]*?AS\s+INT\s*\)",
                r"CAST(LEFT(OrderFY, 4) AS INT)",
                sql,
                flags=re.IGNORECASE
            )
        return sql

    def fix_double_brackets(self, sql: str) -> str:
        """Fix [[Amount]] → [Amount]"""
        return re.sub(r"\[\[([^\]]+)\]\]", r"[\1]", sql)

    def fix_missing_group_by(self, sql: str) -> str:
        """Auto-add GROUP BY if aggregation used but missing."""
        has_agg = bool(re.search(r"\bSUM\(|COUNT|AVG|MIN|MAX", sql, re.IGNORECASE))
        has_group_by = "GROUP BY" in sql.upper()
        if has_agg and not has_group_by:
            # Look for non-aggregated expressions
            match = re.search(r"SELECT\s+.*?,?\s*(\w+|\([^)]+\))\s+AS", sql, re.IGNORECASE)
            if match:
                col = match.group(1)
                if "ORDER BY" in sql.upper():
                    sql = re.sub(r"\s+ORDER BY", f"\nGROUP BY {col}\nORDER BY", sql, flags=re.IGNORECASE)
                else:
                    sql += f"\nGROUP BY {col}"
                logger.warning(f"Added missing GROUP BY: {col}")
        return sql

    def repair_sql(self, sql: str) -> str:
        """
        Apply all fixes in order.
        """
        fixes = [
            self.fix_double_brackets,
            self.fix_column_names,
            self.fix_cast_fy,
            self.fix_top_placement,
            self.fix_missing_group_by,
        ]
        for fix in fixes:
            sql = fix(sql)
        return sql.strip()

    def validate_sql(self, sql: str) -> bool:
        # Logic check first
        logic_ok, reason = self.validate_logic(sql)
        if not logic_ok:
            logger.error(f"Invalid SQL logic: {reason} | SQL: {sql}")
            return False

        # Column check
        col_ok, invalid_cols = self.validate_columns(sql)
        if not col_ok:
            logger.error(f"Invalid columns: {invalid_cols}")
            return False

        # Basic structure
        if not sql.strip().upper().startswith("SELECT"):
            return False

        return True
    def validate_logic(self, sql: str) -> Tuple[bool, str]:
        """
        Detect dangerous or incorrect SQL patterns.
        """
        sql_lower = sql.lower()

        # Rule 1: Raw Amount without aggregation
        if re.search(r"SELECT\s+(?:TOP \d+\s+)?\[?amount\]?\s+FROM", sql_lower):
            return False, "Raw [Amount] selected without aggregation"

        # Rule 2: UNION for comparison (should use CASE)
        if "union" in sql_lower and "compare" in sql_lower:
            return False, "UNION used for comparison — use CASE WHEN instead"

        # Rule 3: Using OrderFY to filter by month-year
        if re.search(r"cast.*?orderfy.*?int", sql_lower):
            return False, "Do not use CAST(OrderFY AS INT) to filter by month-year"

        # Rule 4: Using MonthName without MMMMYY when available
        if "[MonthName]" in sql and "[MMMMYY]" in self.valid_columns:
            return False, "Use [MMMMYY] for month-year filtering"

        return True, "OK"