# intent_router.py

import re
from typing import Dict, Optional
from datetime import datetime, date, timedelta
import json

# -------------------------------
# Load Synonym Map
# -------------------------------

def load_synonym_map(path="synonym_map.json"):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        word_to_col = {}
        for col, synonyms in data.get("columns", {}).items():
            for syn in synonyms:
                word_to_col[syn.lower().strip()] = col
        return word_to_col
    except Exception as e:
        print(f"[ERROR] Failed to load synonym_map.json: {e}")
        return {}

SYNONYM_MAP = load_synonym_map()

# -------------------------------
# Helpers
# -------------------------------

def resolve_column(text: str) -> str:
    if not text:
        return None
    text = text.lower().strip()
    return SYNONYM_MAP.get(text, None)

def resolve_fy_hint(hint: str) -> str:
    hint = hint.lower().strip()
    today = datetime.now()
    year = today.year
    month = today.month
    current_fy = f"{year}-{str(year+1)[-2:]}" if month >= 4 else f"{year-1}-{str(year)[-2:]}"
    
    if "current" in hint:
        return current_fy
    elif "previous" in hint:
        curr_start = int(current_fy.split("-")[0])
        return f"{curr_start-1}-{str(curr_start)[-2:]}"
    elif re.match(r"20\d{2}-\d{2}", hint):
        return hint
    return None

def normalize_month_year(text: str) -> str:
    month_map = {
        'january': 'Jan', 'february': 'Feb', 'march': 'Mar',
        'april': 'Apr', 'may': 'May', 'june': 'Jun',
        'july': 'Jul', 'august': 'Aug', 'september': 'Sep',
        'october': 'Oct', 'november': 'Nov', 'december': 'Dec'
    }
    for full, abbr in month_map.items():
        if full in text.lower():
            year_match = re.search(r"(\d{4})", text)
            if year_match:
                return f"{abbr}-{year_match.group(1)[2:]}"
    return text.title()

def extract_value_after(q: str, keyword: str, pattern: str) -> str:
    match = re.search(rf"{keyword}\s+{pattern}", q, re.I)
    return match.group(1).strip() if match else None

# -------------------------------
# Unified Intent Parser
# -------------------------------

def parse_intent(q: str) -> Dict:
    """
    Parse full query into structured intent.
    No premature decisions.
    """
    q = q.lower().strip()
    intent = {
        "verb": None,
        "metric": "Amount",
        "group_by": [],
        "columns": [],  # for list rows
        "filters": []
    }

    # 1. Detect Verb
    if "compare" in q:
        intent["verb"] = "compare"
    elif "growth" in q:
        intent["verb"] = "growth"
    elif "top" in q or "list" in q:
        intent["verb"] = "top"
    elif any(kw in q for kw in ["list of", "show me", "date,", "no,", "customer,"]):
        intent["verb"] = "list_rows"
    else:
        intent["verb"] = "aggregate"

    # 2. Metric
    if "invoiced quantity" in q:
        intent["metric"] = "InvoicedQuantity"
    elif "quantity" in q:
        intent["metric"] = "Quantity"
    elif "backlog" in q:
        intent["metric"] = "BacklogAmount"
    else:
        intent["metric"] = "Amount"

    # 3. Group By / Columns
    by_match = re.search(r"by\s+([\w\s\-]+?)(?:\s+(?:and|for|in|$))", q)
    if by_match:
        parts = re.split(r"\s+and\s+|\s*,\s+", by_match.group(1).strip())
        for part in parts:
            col = resolve_column(part.strip())
            if col:
                intent["group_by"].append(col)

    # 4. Columns (for list)
    if intent["verb"] == "list_rows":
        col_names = re.findall(r"(no|date|customer|amount|total amount)", q)
        for c in col_names:
            col = resolve_column(c) or c.title()
            if col not in intent["columns"]:
                intent["columns"].append(col)

    # 5. Filters
    # a. FY
    fy_hint = extract_value_after(q, r"(?:for|in)\s+fy", r"(20\d{2}-\d{2}|current|previous)")
    if not fy_hint:
        fy_hint = "current" if "current" in q else "previous" if "previous" in q else None
    if fy_hint:
        resolved = resolve_fy_hint(fy_hint)
        if resolved:
            intent["filters"].append(f"OrderFY = '{resolved}'")

    # b. Previous Month
    if "previous month" in q:
        prev_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%b-%y")
        intent["filters"].append(f"UPPER([monthyear]) = UPPER('{prev_month}')")

    # c. MFGMode
    mfg_match = re.search(r"mfg\s+is\s+([\w-]+)", q, re.I)
    if mfg_match:
        intent["filters"].append(f"[MFGMode] = '{mfg_match.group(1).title()}'")

    # d. Customer
    cust_match = re.search(r"customer\s+is\s+([A-Z][\w\s.&-]+?)(?:\s+|$)", q, re.I)
    if cust_match:
        intent["filters"].append(f"[Customer_Name] = '{cust_match.group(1).strip()}'")

    # e. Type
    type_match = re.search(r"type\s+is\s+(\w+)", q, re.I)
    if type_match:
        intent["filters"].append(f"[Type] = '{type_match.group(1)}'")

    # f. Month-Year Range (for growth)
    if intent["verb"] == "growth":
        month_match = re.search(r"between\s+(\w+\s+\d{4})\s+and\s+(\w+\s+\d{4})", q, re.I)
        if month_match:
            start, end = month_match.groups()
            start_my = normalize_month_year(start)
            end_my = normalize_month_year(end)
            intent["period1"] = start_my
            intent["period2"] = end_my

    return intent

# -------------------------------
# SQL Generators
# -------------------------------

def generate_sql_from_intent(intent: Dict) -> str:
    verb = intent["verb"]
    metric = intent["metric"]
    filters = intent["filters"]
    where_sql = " WHERE " + " AND ".join(filters) if filters else ""

    if verb == "aggregate":
        group_by = intent["group_by"] or ["OrderFY"]
        select_cols = ", ".join(f"[{col}]" for col in group_by)
        return f"""
SELECT
    {select_cols},
    SUM([{metric}]) AS TotalAmount
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY {select_cols}
ORDER BY TotalAmount DESC
"""

    elif verb == "compare":
        val1 = extract_value_after(intent["raw"], "in", r"(.+?)\s+(?:and|vs)")
        val2 = extract_value_after(intent["raw"], r"(?:and|vs)", r"(.+)")
        col = "OrderFY" if re.match(r"20\d{2}-\d{2}", val1) else "monthyear"
        return f"""
SELECT
    SUM(CASE WHEN {col} = '{val1}' THEN {metric} ELSE 0 END) AS [{val1}],
    SUM(CASE WHEN {col} = '{val2}' THEN {metric} ELSE 0 END) AS [{val2}]
FROM dbo.SalesPlanTable
WHERE {col} IN ('{val1}', '{val2}')
{where_sql}
"""

    elif verb == "growth":
        p1 = intent.get("period1")
        p2 = intent.get("period2")
        if p1 and p2:
            return f"""
SELECT
    SUM(CASE WHEN [monthyear] = '{p1}' THEN Amount ELSE 0 END) AS BaseAmount,
    SUM(CASE WHEN [monthyear] = '{p2}' THEN Amount ELSE 0 END) AS NewAmount,
    (SUM(CASE WHEN [monthyear] = '{p2}' THEN Amount ELSE 0 END) - 
     SUM(CASE WHEN [monthyear] = '{p1}' THEN Amount ELSE 0 END)) AS Absolute_Growth,
    CASE 
        WHEN SUM(CASE WHEN [monthyear] = '{p1}' THEN Amount ELSE 0 END) > 0
        THEN (SUM(CASE WHEN [monthyear] = '{p2}' THEN Amount ELSE 0 END) - 
              SUM(CASE WHEN [monthyear] = '{p1}' THEN Amount ELSE 0 END)) * 100.0 / 
             SUM(CASE WHEN [monthyear] = '{p1}' THEN Amount ELSE 0 END)
        ELSE NULL 
    END AS Pct_Growth
FROM dbo.SalesPlanTable
WHERE [monthyear] IN ('{p1}', '{p2}')
{where_sql}
"""

    elif verb == "top":
        n = re.search(r"top\s+(\d+)", intent["raw"], re.I)
        n = n.group(1) if n else "10"
        entity = intent["group_by"][0] if intent["group_by"] else "Customer_Name"
        return f"""
SELECT TOP {n}
    [{entity}],
    SUM([{metric}]) AS Total{metric}
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [{entity}]
ORDER BY Total{metric} DESC
"""

    elif verb == "list_rows":
        cols = intent["columns"] or ["DocumentNo", "OrderDate", "Customer_Name", "Amount"]
        select_cols = ", ".join(f"[{c}]" for c in cols)
        if "total amount" in intent["raw"]:
            group_cols = ", ".join(f"[{c}]" for c in cols if c != "Amount")
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
ORDER BY OrderDate DESC
"""

    return None

# -------------------------------
# Main Entry Point
# -------------------------------

def generate_sql(question: str, schema_text: str = None) -> Optional[str]:
    try:
        intent = parse_intent(question)
        intent["raw"] = question  # for extraction
        return generate_sql_from_intent(intent)
    except Exception as e:
        print(f"[DEBUG] Parse failed: {e}")
        return None