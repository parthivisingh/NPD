# intent_router.py

import re
from typing import Dict, List, Optional
from datetime import datetime, date, timedelta
import json

ORDER_BY_CANDIDATES = ["OrderDate", "Date", "Order_Date", "DocumentDate"]

def has_column(conn, col: str) -> bool:
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'SalesPlanTable' AND COLUMN_NAME = ?
    """, col)
    return cur.fetchone() is not None

# -------------------------------
# Load Synonym Map
# -------------------------------

def load_synonym_map(path="synonym_map.json"):
    """Load synonym map from JSON file."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"[ERROR] Failed to load synonym_map.json: {e}")
        return {}

SYNONYM_MAP = load_synonym_map()

# -------------------------------
# Helper Functions
# -------------------------------

def resolve_column(text: str) -> str:
    if not text:
        return None
    text = text.lower().strip()
    
    # Build list of (synonym, col) and sort by length (longest first)
    candidates = []
    for col, synonyms in SYNONYM_MAP.get("columns", {}).items():
        for syn in synonyms:
            syn_clean = syn.lower().strip()
            if re.search(rf"\b{re.escape(syn_clean)}\b", text):
                candidates.append((syn_clean, col))
    
    # Sort by synonym length (longest first)
    candidates.sort(key=lambda x: len(x[0]), reverse=True)
    return candidates[0][1] if candidates else None

def resolve_fy_hint(hint: str) -> str:
    """
    Resolve FY hints like 'current', 'previous', or '2024-25'
    """
    hint = hint.lower().strip()
    today = datetime.now()
    year = today.year
    month = today.month

    current_fy = f"{year}-{str(year+1)[-2:]}" if month >= 4 else f"{year-1}-{str(year)[-2:]}"

    if "current" in hint:
        return current_fy
    elif "previous" in hint:
        curr_start = int(current_fy.split("-")[0])
        prev_start = curr_start - 1
        return f"{prev_start}-{str(curr_start)[-2:]}"
    elif re.match(r"20\d{2}-\d{2}", hint):
        return hint
    return None

def normalize_my(text: str) -> str:
    """
    Convert 'august 2024' → 'Aug-24'
    """
    month_map = {
        'january': 'Jan', 'february': 'Feb', 'march': 'Mar',
        'april': 'Apr', 'may': 'May', 'june': 'Jun',
        'july': 'Jul', 'august': 'Aug', 'september': 'Sep',
        'october': 'Oct', 'november': 'Nov', 'december': 'Dec'
    }
    text = text.lower()
    for full, abbr in month_map.items():
        if full in text:
            year_match = re.search(r"(\d{4})", text)
            if year_match:
                return f"{abbr}-{year_match.group(1)[2:]}"
    # Try 'apr-25'
    match = re.search(r"(\w{3})[-\s](\d{2})", text)
    if match:
        mon, yr = match.groups()
        return f"{mon.title()}-{yr}"
    return text.title()

# -------------------------------
# Filter Extraction
# -------------------------------

def extract_filters(q: str) -> List[str]:
    """
    Extract all filters from the query.
    Returns list of WHERE conditions.
    """
    filters = []
    q_lower = q.lower()

    # OrderFY: current, previous, or explicit
    fy_hint = None
    if "current fy" in q_lower or "is current" in q_lower:
        fy_hint = "current"
    elif "previous fy" in q_lower:
        fy_hint = "previous"
    else:
        fy_match = re.search(r"fy\s+(20\d{2}-\d{2})", q, re.I)
        if fy_match:
            fy_hint = fy_match.group(1)

    if fy_hint:
        fy = resolve_fy_hint(fy_hint)
        if fy:
            filters.append(f"OrderFY = '{fy}'")

    # MFGMode
    mfg_match = re.search(r"mfg\s+is\s+([\w-]+)", q, re.I)
    if mfg_match:
        filters.append(f"[MFGMode] = '{mfg_match.group(1).title()}'")

    # Customer_Name
    cust_match = re.search(r"customer\s+is\s+([A-Z][\w\s.&-]+?)(?:\s+|$)", q, re.I)
    if cust_match:
        filters.append(f"[Customer_Name] = '{cust_match.group(1).strip()}'")

    # Previous Month
    if "previous month" in q_lower:
        prev_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%b-%y")
        filters.append(f"[monthyear] = '{prev_month}'")

    # Quarter
    q_match = re.search(r"quarter\s+is\s+(Q[1-4])", q, re.I)
    if q_match:
        filters.append(f"[PlannedQuarter] = '{q_match.group(1)}'")

    return filters

# -------------------------------
# Intent Detection
# -------------------------------

def detect_intent(q: str) -> str:
    """
    Detect high-level intent.
    Order matters: high-signal verbs first.
    """
    q = q.lower().strip()

    if any(word in q for word in ["compare", "vs", "versus"]):
        return "compare"
    if any(word in q for word in ["growth", "increase", "delta", "change"]):
        return "growth"
    if "top" in q or any(word in q for word in ["best", "highest", "largest"]):
        return "top_n"
    if any(phrase in q for phrase in ["list of", "show me", "give me", "retrieve"]):
        return "list_rows"
    if "count of" in q or "number of" in q:
        return "count"
    if "total amount" in q or "sum of" in q:
        return "total"
    if "amount by" in q or "sales by" in q:
        return "aggregate"

    return "unknown"

# -------------------------------
# Entity Extraction
# -------------------------------

def extract_entities(q: str) -> List[str]:
    # Match "by X" up to comma, "and", or end
    by_match = re.search(r"by\s+([^,;]+?)(?:\s*(?:,|and|$))", q, re.I)
    if not by_match:
        return []
    text = by_match.group(1).strip()
    parts = re.split(r"\s+and\s+|\s*,\s+", text)
    entities = []
    for part in parts:
        col = resolve_column(part.strip())
        if col:
            entities.append(col)
    return entities
# -------------------------------
# Main SQL Generator
# -------------------------------

def generate_sql(question: str, schema_text: str = None) -> Optional[str]:
    """
    Main entry point: detect intent and return SQL.
    Returns None if no template matches (fallback to LLM).
    """
    q_orig = question
    q = question.lower().strip()

    # Remove visualization hints
    q_clean = re.sub(r"\s*as\s+(chart|matrix|table|stacked\s+bar?)", "", q, flags=re.I)
    q_clean = re.sub(r"\s*sort by\s+\w+", "", q_clean, flags=re.I).strip()

    intent = detect_intent(q_clean)

    # -------------------------------
    # 1. Compare: "Compare Amount in FY 2023-24 vs 2024-25"
    # -------------------------------
    if intent == "compare":
        match = re.search(r"compare\s+amount\s+in\s+(?:fy\s+)?(.+?)\s+(?:and|vs)\s+(.+)", q_clean, re.I)
        if match:
            val1_raw, val2_raw = match.groups()
            val1 = val1_raw.strip().strip("'\"")
            val2 = val2_raw.strip().strip("'\"")

            # Case 1: FY
            if re.match(r"20\d{2}-\d{2}", val1) and re.match(r"20\d{2}-\d{2}", val2):
                return f"""
SELECT
    SUM(CASE WHEN OrderFY = '{val1}' THEN Amount ELSE 0 END) AS [{val1}],
    SUM(CASE WHEN OrderFY = '{val2}' THEN Amount ELSE 0 END) AS [{val2}]
FROM dbo.SalesPlanTable
WHERE OrderFY IN ('{val1}', '{val2}')
"""

            # Case 2: monthyear
            if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b", val1, re.I):
                my1 = normalize_my(val1)
                my2 = normalize_my(val2)
                return f"""
SELECT
    SUM(CASE WHEN [monthyear] = '{my1}' THEN Amount ELSE 0 END) AS [{my1}],
    SUM(CASE WHEN [monthyear] = '{my2}' THEN Amount ELSE 0 END) AS [{my2}]
FROM dbo.SalesPlanTable
WHERE [monthyear] IN ('{my1}', '{my2}')
"""

    # -------------------------------
    # 2. Growth: "growth between august 2024 and july 2025"
    # -------------------------------
    if intent == "growth":
        match = re.search(r"between\s+(.+?)\s+(?:and|to)\s+(.+)", q_clean, re.I)
        if match:
            start_raw, end_raw = match.groups()
            start = normalize_my(start_raw.strip())
            end = normalize_my(end_raw.strip())

            return f"""
SELECT
    SUM(CASE WHEN [monthyear] = '{start}' THEN Amount ELSE 0 END) AS BaseAmount,
    SUM(CASE WHEN [monthyear] = '{end}' THEN Amount ELSE 0 END) AS NewAmount,
    (SUM(CASE WHEN [monthyear] = '{end}' THEN Amount ELSE 0 END) - 
     SUM(CASE WHEN [monthyear] = '{start}' THEN Amount ELSE 0 END)) AS Absolute_Growth,
    CASE 
        WHEN SUM(CASE WHEN [monthyear] = '{start}' THEN Amount ELSE 0 END) > 0
        THEN (SUM(CASE WHEN [monthyear] = '{end}' THEN Amount ELSE 0 END) - 
              SUM(CASE WHEN [monthyear] = '{start}' THEN Amount ELSE 0 END)) * 100.0 / 
             SUM(CASE WHEN [monthyear] = '{start}' THEN Amount ELSE 0 END)
        ELSE NULL 
    END AS Pct_Growth
FROM dbo.SalesPlanTable
WHERE [monthyear] IN ('{start}', '{end}')
"""

    # -------------------------------
    # 3. Top N: "List top 10 customers by amount in FY 2025-26"
    # -------------------------------
    if intent == "top_n":
        match = re.search(r"top\s+(\d+)\s+(.+?)\s+by\s+(.+?)(?:\s+in\s+fy|\s+for\s+fy)?\s*(20\d{2}-\d{2}|current|previous)?", q_clean, re.I)
        if match:
            n, entity_hint, metric_hint, fy_hint = match.groups()
            entity = resolve_column(entity_hint.strip()) or "Customer_Name"
            metric = resolve_column(metric_hint.strip()) or "Amount"

            fy = resolve_fy_hint(fy_hint) if fy_hint else None
            where_sql = f" WHERE OrderFY = '{fy}'" if fy else ""

            return f"""
SELECT TOP {n}
    [{entity}],
    SUM([{metric}]) AS Total{metric}
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [{entity}]
ORDER BY Total{metric} DESC
"""

    # -------------------------------
    # 4. List Rows: "list of no, date, customer..."
    # -------------------------------
    if intent == "list_rows":
        cols = re.findall(r"(no|date|customer|amount)", q_clean, re.I)
        mapped_cols = [resolve_column(c) or c.title() for c in cols]
        if not mapped_cols:
            return None

        select_cols = ", ".join(f"[{c}]" for c in mapped_cols)
        filters = extract_filters(q_clean)
        where_sql = " WHERE " + " AND ".join(filters) if filters else ""

        if "total amount" in q_clean:
            # Aggregate by non-Amount columns
            group_cols = ", ".join(f"[{c}]" for c in mapped_cols if c != "Amount")
            if not group_cols:
                return f"""
    SELECT
        SUM(Amount) AS TotalAmount
    FROM dbo.SalesPlanTable
    {where_sql}
    """
            return f"""
    SELECT
        {select_cols}, SUM(Amount) AS TotalAmount
    FROM dbo.SalesPlanTable
    {where_sql}
    GROUP BY {group_cols}
    ORDER BY TotalAmount DESC
    """
        else:
            return f"""
    SELECT
        {select_cols}
    FROM dbo.SalesPlanTable
    {where_sql}
    """

    # -------------------------------
    # 5. Aggregate: "Total amount by FY and type"
    # -------------------------------
    if intent in ["total", "aggregate"]:
        entities = extract_entities(q_clean)
        if not entities:
            # Fallback: try direct resolve
            match = re.search(r"by\s+([\w\s]+?)(?:\s+(?:for|in|$))", q_clean)
            if match:
                col = resolve_column(match.group(1).strip())
                if col:
                    entities = [col]
        if not entities:
            return None

        filters = extract_filters(q_clean)
        where_sql = " WHERE " + " AND ".join(filters) if filters else ""
        select_cols = ", ".join(f"[{col}]" for col in entities)

        return f"""
SELECT
    {select_cols},
    SUM(Amount) AS TotalAmount
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY {select_cols}
ORDER BY TotalAmount DESC
"""

    # -------------------------------
    # 6. Count: "Count of No in month year apr-25"
    # -------------------------------
    if intent == "count":
        match = re.search(r"count of (\w+)", q_clean, re.I)
        if match:
            thing = match.group(1).strip()
            col = resolve_column(thing) or "DocumentNo"

            filters = extract_filters(q_clean)
            where_sql = " WHERE " + " AND ".join(filters) if filters else ""

            return f"""
SELECT
    COUNT(DISTINCT [{col}]) AS Count_{col}
FROM dbo.SalesPlanTable
{where_sql}
"""

    # No template matched
    return None