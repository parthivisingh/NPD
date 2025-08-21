# intent_router.py

import re
from typing import Dict, List, Optional
from datetime import datetime, date, timedelta
import json

# -------------------------------
# Load Synonym Map (do this once)
# -------------------------------

def load_synonym_map(path="synonym_map.json"):
    """Load synonym map from JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Flatten for fast lookup: "sales" -> "Amount"
        word_to_col = {}
        for col, synonyms in data.get("columns", {}).items():
            for syn in synonyms:
                word_to_col[syn.lower().strip()] = col

        intent_map = {}
        for intent, phrases in data.get("intent", {}).items():
            for phrase in phrases:
                intent_map[phrase.lower().strip()] = intent

        metric_map = {}
        for metric, phrases in data.get("metrics", {}).items():
            for phrase in phrases:
                metric_map[phrase.lower().strip()] = metric

        return {
            "word_to_col": word_to_col,
            "intent_map": intent_map,
            "metric_map": metric_map,
            "raw": data
        }
    except Exception as e:
        print(f"[ERROR] Failed to load synonym_map.json: {e}")
        return {
            "word_to_col": {},
            "intent_map": {},
            "metric_map": {}
        }

# Load once at module level
SYNONYM_MAP = load_synonym_map()

# -------------------------------
# Helper Functions
# -------------------------------

def resolve_column(text: str) -> str:
    """
    Resolve a natural language word to a real column name.
    Example: 'fy' -> 'OrderFY', 'customer' -> 'Customer_Name'
    """
    text = text.lower().strip()
    if text in SYNONYM_MAP["word_to_col"]:
        return SYNONYM_MAP["word_to_col"][text]

    # Check partial match
    for key in sorted(SYNONYM_MAP["word_to_col"], key=len, reverse=True):
        if key in text:
            return SYNONYM_MAP["word_to_col"][key]
    return None

def detect_intent(q: str) -> Dict[str, bool]:
    """
    Detect high-level intents: current_fy, previous_fy, etc.
    """
    q = q.lower()
    detected = {}
    for phrase, intent in SYNONYM_MAP["intent_map"].items():
        if phrase in q:
            detected[intent] = True
    return detected

def resolve_fy_hint(q: str) -> str:
    """
    Detect if query wants 'current' or 'previous' FY.
    """
    intents = detect_intent(q)
    today = datetime.now()
    year = today.year
    month = today.month

    if month >= 4:
        current_fy = f"{year}-{str(year+1)[-2:]}"
    else:
        current_fy = f"{year-1}-{str(year)[-2:]}"

    if intents.get("current_fy"):
        return current_fy
    elif intents.get("previous_fy"):
        curr_start = int(current_fy.split("-")[0])
        prev_start = curr_start - 1
        return f"{prev_start}-{str(curr_start)[-2:]}"
    return None

# -------------------------------
# Main SQL Generator
# -------------------------------

def generate_sql(question: str, schema_text: str = None) -> Optional[str]:
    q = question.lower().strip()
    
    # -------------------------------------------------
    # Template 4: Compare Amount by month in FY A and B
    # -------------------------------------------------
    match = re.search(r"compare\s+amount\s+by\s+month\s+in\s+(.+?)\s+(?:and|vs)\s+(.+)", q)
    if match:
        fy1_raw, fy2_raw = match.groups()
        fy1 = fy1_raw.strip().strip("'\"")
        fy2 = fy2_raw.strip().strip("'\"")

        if not re.match(r"20\d{2}-\d{2}", fy1) or not re.match(r"20\d{2}-\d{2}", fy2):
            return None

        return f"""
SELECT 
    [MonthName],
    SUM(CASE WHEN OrderFY = '{fy1}' THEN Amount ELSE 0 END) AS [{fy1}],
    SUM(CASE WHEN OrderFY = '{fy2}' THEN Amount ELSE 0 END) AS [{fy2}]
FROM dbo.SalesPlanTable
WHERE OrderFY IN ('{fy1}', '{fy2}')
GROUP BY [MonthName]
ORDER BY 
    CASE [MonthName]
        WHEN 'April' THEN 1 WHEN 'May' THEN 2 WHEN 'June' THEN 3
        WHEN 'July' THEN 4 WHEN 'August' THEN 5 WHEN 'September' THEN 6
        WHEN 'October' THEN 7 WHEN 'November' THEN 8 WHEN 'December' THEN 9
        WHEN 'January' THEN 10 WHEN 'February' THEN 11 WHEN 'March' THEN 12
    END
"""
    
    # -------------------------------------
    # Template 2: Total amount by X and Y
    # -------------------------------------
    match = re.search(r"total\s+amount\s+by\s+(.+?)\s+(?:and|by)\s+(.+)", q)
    if match:
        col1_hint, col2_hint = match.groups()
        col1 = resolve_column(col1_hint.strip())
        col2 = resolve_column(col2_hint.strip())
        if not col1 or not col2:
            return None

        return f"""
SELECT
    [{col1}], [{col2}],
    SUM(Amount) AS TotalAmount
FROM dbo.SalesPlanTable
GROUP BY [{col1}], [{col2}]
ORDER BY TotalAmount DESC
"""

    # -------------------------------------------------
    # Template 1A: Total amount by X in FY <value>
    # Example: "Total amount by month in FY 2025-26"
    # -------------------------------------------------
    match = re.search(r"total\s+amount\s+by\s+(.+?)\s+(?:in|for)?\s+fy\s+(20\d{2}-\d{2})", q, re.I)
    if match:
        col_hint, fy = match.groups()
        col = resolve_column(col_hint.strip())
        if not col:
            return None

        return f"""
    SELECT
        [{col}],
        SUM(Amount) AS TotalAmount
    FROM dbo.SalesPlanTable
    WHERE OrderFY = '{fy}'
    GROUP BY [{col}]
    ORDER BY TotalAmount DESC
    """

    # -------------------------------------------------
    # Template 1B: Total amount by X (no FY)
    # Example: "Total amount by month"
    # -------------------------------------------------
    match = re.search(r"total\s+amount\s+by\s+(.+)", q)
    if match:
        col_hint = match.group(1).strip()
        col = resolve_column(col_hint)
        if not col:
            return None

        return f"""
    SELECT
        [{col}],
        SUM(Amount) AS TotalAmount
    FROM dbo.SalesPlanTable
    GROUP BY [{col}]
    ORDER BY TotalAmount DESC
    """

    

    # ----------------------------------------
    # Template 3: Amount by X for FY <value>
    # ----------------------------------------
    match = re.search(r"amount\s+by\s+(.+?)\s+(?:in|for)?\s*fy", q)
    if match:
        col_hint = match.group(1).strip()
        col = resolve_column(col_hint)
        if not col:
            return None

        fy = resolve_fy_hint(q)
        if not fy:
            # Try to extract FY like '2024-25'
            fy_match = re.search(r"(20\d{2}-\d{2})", q)
            fy = fy_match.group(1) if fy_match else None
            if not fy:
                return None

        return f"""
SELECT
    [{col}],
    SUM(Amount) AS TotalAmount
FROM dbo.SalesPlanTable
WHERE OrderFY = '{fy}'
GROUP BY [{col}]
ORDER BY TotalAmount DESC
"""


    # -------------------------------------------------
    # Template 5: Compare Amount in month year apr-25 and apr-24
    # -------------------------------------------------
    match = re.search(r"compare\s+amount\s+in\s+month\s+year\s+(\w+)-(\d{2})\s+and\s+(\w+)-(\d{2})", q)
    if match:
        mon1, yr1, mon2, yr2 = match.groups()
        code1 = f"{mon1.strip()[:3].upper()}{yr1}"
        code2 = f"{mon2.strip()[:3].upper()}{yr2}"

        return f"""
SELECT
    SUM(CASE WHEN [MMMMYY] = '{code1}' THEN Amount ELSE 0 END) AS Total_{code1},
    SUM(CASE WHEN [MMMMYY] = '{code2}' THEN Amount ELSE 0 END) AS Total_{code2}
FROM dbo.SalesPlanTable
WHERE [MMMMYY] IS NOT NULL 
  AND [MMMMYY] LIKE '[A-Z][A-Z][A-Z][A-Z][0-9][0-9]'
  AND [MMMMYY] IN ('{code1}', '{code2}')
"""

    # --------------------------------------------
    # Template 6: List top N by X in FY
    # --------------------------------------------
    match = re.search(r"list\s+top\s+(\d+)\s+(.+?)\s+by\s+(.+?)(?:\s+in\s+fy|\s+for\s+fy)?", q)
    if match:
        n, entity_hint, metric_hint = match.groups()
        entity = resolve_column(entity_hint)
        metric = resolve_column(metric_hint) or "Amount"
        if not entity:
            return None

        fy = resolve_fy_hint(q)
        if not fy:
            fy_match = re.search(r"(20\d{2}-\d{2})", q)
            fy = fy_match.group(1) if fy_match else None
            if not fy:
                fy = "2024-25"  # fallback

        return f"""
SELECT TOP {n}
    [{entity}],
    SUM([{metric}]) AS Total{metric}
FROM dbo.SalesPlanTable
WHERE OrderFY = '{fy}'
GROUP BY [{entity}]
ORDER BY Total{metric} DESC
"""

    # No template matched
    return None