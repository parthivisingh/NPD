# intent_router.py

import re
from typing import Dict, List, Optional
from datetime import datetime
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
    if not text:
        return None
    text = text.lower().strip()
    if text in SYNONYM_MAP["word_to_col"]:
        return SYNONYM_MAP["word_to_col"][text]

    # Check partial match (longest first)
    for key in sorted(SYNONYM_MAP["word_to_col"], key=len, reverse=True):
        if key in text:
            return SYNONYM_MAP["word_to_col"][key]
    return None

def detect_intent(q: str) -> Dict[str, str]:
    """
    Detect high-level intents: current_fy, previous_fy.
    Returns resolved values.
    """
    q = q.lower()
    today = datetime.now()
    year = today.year
    month = today.month

    if month >= 4:
        current_fy = f"{year}-{str(year+1)[-2:]}"
    else:
        current_fy = f"{year-1}-{str(year)[-2:]}"

    prev_fy = f"{int(current_fy.split('-')[0]) - 1}-{current_fy.split('-')[1]}"

    if "current fy" in q or "is current" in q:
        return {"fy": current_fy}
    elif "previous fy" in q or "fy previous" in q:
        return {"fy": prev_fy}
    else:
        fy_match = re.search(r"fy\s+(20\d{2}-\d{2})", q)
        if fy_match:
            return {"fy": fy_match.group(1)}
    return {}

# -------------------------------
# Structured Query Parser
# -------------------------------

def parse_query(q: str) -> Dict:
    """
    Parse full query into structured intent.
    No premature decisions.
    """
    q_orig = q
    q = q.lower().strip()
    intent = {
        "metric": "SUM(Amount)",
        "group_by": [],
        "filters": []
    }

    # 1. Metric
    if "total amount" in q:
        intent["metric"] = "SUM(Amount)"
    elif "count of" in q:
        match = re.search(r"count of (\w+)", q)
        col = match.group(1).strip() if match else "DocumentNo"
        resolved = resolve_column(col)
        intent["metric"] = f"COUNT(DISTINCT [{resolved}])" if resolved else "COUNT(*)"
    elif "invoiced quantity" in q:
        intent["metric"] = "SUM(InvoicedQuantity)"
    elif "quantity" in q:
        intent["metric"] = "SUM(Quantity)"

    # 2. Group By
    by_match = re.search(r"by\s+([\w\s\-]+?)(?:\s+(?:and|for|in|where|$))", q)
    if by_match:
        parts = re.split(r"\s+and\s+|\s*,\s+", by_match.group(1).strip())
        for part in parts:
            col = resolve_column(part.strip())
            if col:
                intent["group_by"].append(col)

    # 3. Filters
    # a. FY from intent
    fy_intent = detect_intent(q)
    if "fy" in fy_intent:
        intent["filters"].append(f"OrderFY = '{fy_intent['fy']}'")

    # b. Month range: "month is April to June"
    month_match = re.search(r"month is (\w+) to (\w+)", q, re.I)
    if month_match:
        start, end = month_match.groups()
        month_order = [
            "January", "February", "March",
            "April", "May", "June",
            "July", "August", "September",
            "October", "November", "December"
        ]
        try:
            start_idx = month_order.index(start.title())
            end_idx = month_order.index(end.title()) + 1
            months = month_order[start_idx:end_idx]
            month_list = "', '".join(months)
            intent["filters"].append(f"[MonthName] IN ('{month_list}')")
        except ValueError:
            pass

    # c. MFGMode: "mfg is production"
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

    # f. monthyear: "apr-25"
    my_match = re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-(\d{2})\b", q, re.I)
    if my_match:
        mon, yr = my_match.groups()
        val = f"{mon.title()}-{yr}"
        intent["filters"].append(f"[monthyear] = '{val}'")

    return intent

def generate_sql_from_intent(intent: Dict) -> str:
    """
    Build SQL from structured intent.
    """
    metric = intent["metric"]
    group_by = intent["group_by"]
    filters = intent["filters"]

    # Default group_by
    if not group_by:
        group_by = ["OrderFY"]  # fallback

    # SELECT
    select_cols = ", ".join(f"[{col}]" for col in group_by)
    select_sql = f"SELECT {select_cols}, {metric} AS TotalAmount"

    # FROM
    from_sql = "FROM dbo.SalesPlanTable"

    # WHERE
    where_sql = " WHERE " + " AND ".join(filters) if filters else ""

    # GROUP BY
    group_sql = " GROUP BY " + ", ".join(f"[{col}]" for col in group_by)

    # ORDER BY
    if "MonthName" in group_by:
        order_sql = """
ORDER BY 
    CASE [MonthName]
        WHEN 'April' THEN 1 WHEN 'May' THEN 2 WHEN 'June' THEN 3
        WHEN 'July' THEN 4 WHEN 'August' THEN 5 WHEN 'September' THEN 6
        WHEN 'October' THEN 7 WHEN 'November' THEN 8 WHEN 'December' THEN 9
        WHEN 'January' THEN 10 WHEN 'February' THEN 11 WHEN 'March' THEN 12
    END
"""
    else:
        order_sql = " ORDER BY TotalAmount DESC"

    return f"{select_sql}\n{from_sql}\n{where_sql}\n{group_sql}\n{order_sql}"

# -------------------------------
# Main Entry Point
# -------------------------------

def generate_sql(question: str, schema_text: str = None) -> Optional[str]:
    """
    Parse full query and generate SQL.
    Returns None only if parsing fails.
    """
    try:
        intent = parse_query(question)
        return generate_sql_from_intent(intent)
    except Exception as e:
        print(f"[DEBUG] Intent parsing failed: {e}")
        return None  # fallback to LLM