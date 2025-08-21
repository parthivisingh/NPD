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
        self.column_fix_map = self._build_column_fix_map()

    def _get_valid_columns(self) -> set:
        """Fetch real column names as a set for fast lookup."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = 'SalesPlanTable'
        """)
        return {row[0] for row in cur.fetchall()}

    def _build_column_fix_map(self) -> dict:
        """
        Map common LLM mistakes to correct column names.
        Keys are typos; values are correct column names.
        """
        return {
            # MMMYY variants
            "mmmmyy": "MMMMYY",
            "mmmyy": "MMMMYY",
            "mmmy": "MMMMYY",
            "mmyy": "MMMMYY",
            "my": "monthyear",
            # OrderFY
            "ord_fy": "OrderFY",
            "orderfy": "OrderFY",
            "fy": "OrderFY",
            "ordfy": "OrderFY",
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
            # Misc
            "mpcode": "MPCODE",
            "docno": "DocumentNo",
            "no": "DocumentNo",
            "qty": "Quantity",
            "quantity": "Quantity"
        }

    def _resolve_column(self, col: str) -> str:
        """Resolve a column name typo to the correct one."""
        clean = re.sub(r"[\[\]\s]", "", col).lower()
        return self.column_fix_map.get(clean, col)

    def _fix_column_names(self, sql: str) -> str:
        """Fix known column name typos (e.g., [MMMYY] → [MMMMYY])."""
        original = sql
        changed = False

        # Match [col] or bare col, but skip keywords
        tokens = re.finditer(r"\[\s*([^\]]+)\s*\]|\b([a-zA-Z_][\w]*)\b", sql)
        for match in reversed(list(tokens)):
            inner = match.group(1) or match.group(2)

            # Skip SQL keywords
            if inner.upper() in {
                "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "TOP",
                "SUM", "COUNT", "AVG", "MIN", "MAX", "CAST", "INT", "AS",
                "AND", "OR", "IN", "LIKE", "IS", "NULL", "NOT", "UNION",
                "CASE", "WHEN", "THEN", "ELSE", "END", "OVER", "PARTITION",
                "JOIN", "ON", "USING", "WITH", "INTO", "WHEN", "THEN"
            }:
                continue

            fixed = self._resolve_column(inner)
            if fixed != inner:
                start, end = match.span()
                replacement = f"[{fixed}]"
                sql = sql[:start] + replacement + sql[end:]
                changed = True

        if changed:
            logger.info(f"Fixed column names:\nOriginal: {original}\nFixed: {sql}")
        return sql

    def _fix_cast_fy(self, sql: str) -> str:
        """Fix CAST(OrderFY AS INT) → LEFT(OrderFY, 4) for '2024-25' format."""
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
        if "TOP" in sql.upper():
            sql = re.sub(r"\s+TOP\s+\d+", "", sql, flags=re.IGNORECASE)
        return re.sub(r"^SELECT\b", "SELECT TOP 100", sql, flags=re.IGNORECASE)

    def _fix_double_brackets(self, sql: str) -> str:
        """Fix [[Amount]] → [Amount]"""
        return re.sub(r"\[\[([^\]]+)\]\]", r"[\1]", sql)

    def repair_sql(self, sql: str) -> str:
        """
        Apply minimal, safe fixes.
        Order matters: fix brackets → columns → CAST → TOP
        """
        fixes = [
            self._fix_double_brackets,
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
        Light validation: only check for safety, not correctness.
        Let templates handle correctness.
        """
        if not sql:
            return False

        # Remove comments
        cleaned = re.sub(r"--.*?$|/\*.*?\*/", "", sql, flags=re.MULTILINE | re.DOTALL)
        cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()

        if not cleaned.startswith("select"):
            return False

        # Block write operations
        write_keywords = ("insert", "update", "delete", "alter", "drop", "create", "merge", "exec")
        if any(kw in cleaned for kw in write_keywords):
            return False

        return True