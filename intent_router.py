# intent_router.py

import re
from typing import Dict, List, Optional
from datetime import datetime, date, timedelta

def resolve_fy(fy_hint: str, current_fy: str = None) -> str:
    """
    Resolve FY hints like 'current', 'previous' to actual '2024-25' format.
    Assumes April–March fiscal year (India standard).
    Example:
        - If today = Feb 2025 → current FY = 2024-25
        - If today = Aug 2025 → current FY = 2025-26
    """
    if current_fy is None:
        today = datetime.now()
        year = today.year
        month = today.month

        if month >= 4:  # April or later → current FY starts this year
            start_year = year
            end_year = year + 1
        else:  # Jan–March → current FY started last year
            start_year = year - 1
            end_year = year

        current_fy = f"{start_year}-{str(end_year)[-2:]}"

    fy_hint = fy_hint.lower().strip()
    if fy_hint == "current":
        return current_fy
    elif fy_hint == "previous":
        curr_start = int(current_fy.split("-")[0])
        prev_start = curr_start - 1
        return f"{prev_start}-{str(curr_start)[-2:]}"
    else:
        # Assume it's a direct FY string like '2023-24'
        return fy_hint

def resolve_month_year(my_hint: str) -> str:
    """
    Convert 'apr-24' → 'Apr-24' or handle 'previous month'
    """
    if my_hint.lower() == "previous month":
        today = date.today()
        first = today.replace(day=1)
        last_month = first - timedelta(days=1)
        return last_month.strftime("%b-%y").replace(".", "")
    return my_hint.strip().title()

def extract_filters(question: str) -> Dict[str, str]:
    """
    Extract common filters: MFGMode, Customer_Name, monthyear, etc.
    """
    filters = {}

    # MFGMode: "mfg is production"
    #mfg_match = re.search(r"mfg\s+is\s+([a-zA-Z\s]+?)(?:\s+and|\s+as|\s*$)", question, re.I)
    mfg_match = re.search(r"mfg\s+is\s+([\w-]+)", question, re.I)

    if mfg_match:
        filters["MFGMode"] = mfg_match.group(1).strip().title()

    # Customer_Name: "customer is ELTA SYSTEMS LTD"
    #cust_match = re.search(r"customer\s+is\s+([A-Z\s]+?)(?:\s+and|\s+as|\s*$)", question, re.I)
    cust_match = re.search(r"customer\s+is\s+([\w\s.&-]+?)(?:\s+and|\s+as|\s*$)", question, re.I)

    if cust_match:
        filters["Customer_Name"] = cust_match.group(1).strip()

    # Type: "type is Order"
    type_match = re.search(r"type\s+is\s+(\w+)", question, re.I)
    if type_match:
        filters["Type"] = type_match.group(1).strip()

    # monthyear: "in apr-24" or "apr-24"
    my_match = re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)-(\d{2})\b", question, re.I)
    if my_match:
        mon, yr = my_match.groups()
        filters["monthyear"] = f"{mon.title()}-{yr}"

    # MonthName: "month is April"
    mn_match = re.search(r"month\s+is\s+(\w+)", question, re.I)
    if mn_match:
        filters["MonthName"] = mn_match.group(1).strip().title()

    return filters

def build_where_clause(filters: Dict[str, str]) -> str:
    """
    Build WHERE clause from filters.
    """
    conditions = []
    for col, val in filters.items():
        if col == "monthyear":
            conditions.append(f"[{col}] = '{val}'")
        else:
            conditions.append(f"[{col}] = '{val}'")
    return " AND ".join(conditions)

def generate_sql(question: str, schema_text: str = None) -> Optional[str]:
    """
    Main entry point: detect intent and return SQL.
    Returns None if no template matches (fallback to LLM).
    """
    q = question.lower().strip()

    # Always extract filters
    filters = extract_filters(question)
    where_extra = build_where_clause(filters)
    where_parts = []
    
    # -------------------------------------------------
    # Template 4: Compare Amount by month in A and B
    # Example: "Compare Amount by month in FY 2024-25 and 2025-26"
    # -------------------------------------------------
    #match = re.search(r"compare\s+amount\s+by\s+month\s+in\s+(.+?)\s+(?:and|vs)\s+(.+)", q, re.I)
    match=re.search(r"compare\s+amount\s+by\s+month\s+in\s+(?:fy\s+)?(.+?)\s+(?:and|vs)\s+(.+)",q,re.I)
    if match:
        fy1_raw, fy2_raw = match.groups()
        fy1 = fy1_raw.strip().strip("'\"")
        fy2 = fy2_raw.strip().strip("'\"")

        # Validate format
        if not re.match(r"20\d{2}-\d{2}", fy1) or not re.match(r"20\d{2}-\d{2}", fy2):
            return None  # Let LLM handle if not valid FY

        where_parts.append(f"OrderFY IN ('{fy1}', '{fy2}')")
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""

        return f"""
SELECT 
    [MonthName],
    SUM(CASE WHEN OrderFY = '{fy1}' THEN Amount ELSE 0 END) AS [{fy1}],
    SUM(CASE WHEN OrderFY = '{fy2}' THEN Amount ELSE 0 END) AS [{fy2}]
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [MonthName]
ORDER BY 
    CASE [MonthName]
        WHEN 'April' THEN 1
        WHEN 'May' THEN 2
        WHEN 'June' THEN 3
        WHEN 'July' THEN 4
        WHEN 'August' THEN 5
        WHEN 'September' THEN 6
        WHEN 'October' THEN 7
        WHEN 'November' THEN 8
        WHEN 'December' THEN 9
        WHEN 'January' THEN 10
        WHEN 'February' THEN 11
        WHEN 'March' THEN 12
    END
"""

    # -------------------------------------
    # Template 2: Total amount by X and Y
    # -------------------------------------
    match = re.search(r"total\s+amount\s+by\s+(\w+).*?\b(?:and|by)\b\s*(\w+)", q)
    if match:
        col1, col2 = match.groups()
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        return f"""
SELECT
    [{col1}], [{col2}],
    SUM(Amount) AS TotalAmount
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [{col1}], [{col2}]
ORDER BY TotalAmount DESC
"""

    # -----------------------------
    # Template 1: Total amount by X
    # -----------------------------
    match = re.search(r"total\s+amount\s+by\s+(\w+)", q)
    if match:
        col = match.group(1)
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        return f"""
SELECT
    [{col}],
    SUM(Amount) AS TotalAmount
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [{col}]
ORDER BY TotalAmount DESC
"""

    # ----------------------------------------
    # Template 3: Amount by X for FY <value>
    # ----------------------------------------
    match = re.search(r"amount\s+by\s+(\w+).*?fy.*?(20\d{2}-\d{2}|current|previous)", q)
    if match:
        col, fy_hint = match.groups()
        fy = resolve_fy(fy_hint)
        where_parts.append(f"OrderFY = '{fy}'")
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""
        return f"""
SELECT
    [{col}],
    SUM(Amount) AS TotalAmount
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [{col}]
ORDER BY TotalAmount DESC
"""


    # --------------------------------------------
    # Template 5: Growth between A and B (MoM or custom)
    # --------------------------------------------
    match = re.search(r"growth.*?\b(?:between|from)\b\s+(.+?)\s+(?:and|to)\s+(.+)", q, re.I)
    if match:
        val1_raw, val2_raw = match.groups()
        val1_clean = val1_raw.strip().strip("'\"").lower()
        val2_clean = val2_raw.strip().strip("'\"").lower()

        # Map month names to standard format: 'august 2024' → 'Aug-24'
        month_map = {
            'january': 'Jan', 'february': 'Feb', 'march': 'Mar',
            'april': 'Apr', 'may': 'May', 'june': 'Jun',
            'july': 'Jul', 'august': 'Aug', 'september': 'Sep',
            'october': 'Oct', 'november': 'Nov', 'december': 'Dec'
        }

        def normalize_month_year(text: str) -> str:
            for full, abbr in month_map.items():
                if full in text:
                    # Extract year
                    year_match = re.search(r"(\d{4})", text)
                    if year_match:
                        year = year_match.group(1)
                        return f"{abbr}-{year[2:]}"
            return None

        norm1 = normalize_month_year(val1_clean)
        norm2 = normalize_month_year(val2_clean)

        if not norm1 or not norm2:
            return None  # Let LLM handle

        where_parts.append(f"[monthyear] IN ('{norm1}', '{norm2}')")
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""

        return f"""
SELECT
    SUM(CASE WHEN [monthyear] = '{norm1}' THEN Amount ELSE 0 END) AS BaseAmount,
    SUM(CASE WHEN [monthyear] = '{norm2}' THEN Amount ELSE 0 END) AS NewAmount,
    (SUM(CASE WHEN [monthyear] = '{norm2}' THEN Amount ELSE 0 END) - 
     SUM(CASE WHEN [monthyear] = '{norm1}' THEN Amount ELSE 0 END)) AS Absolute_Growth,
    CASE 
        WHEN SUM(CASE WHEN [monthyear] = '{norm1}' THEN Amount ELSE 0 END) > 0
        THEN (SUM(CASE WHEN [monthyear] = '{norm2}' THEN Amount ELSE 0 END) - 
              SUM(CASE WHEN [monthyear] = '{norm1}' THEN Amount ELSE 0 END)) * 100.0 / 
             SUM(CASE WHEN [monthyear] = '{norm1}' THEN Amount ELSE 0 END)
        ELSE NULL 
    END AS Pct_Growth
FROM dbo.SalesPlanTable
{where_sql}
"""
    # --------------------------------------------
    # Template 6: List top N by X in FY
    # --------------------------------------------
    match = re.search(r"list\s+top\s+(\d+)\s+(\w+).*?by\s+(\w+).*?fy.*?(20\d{2}-\d{2}|current|previous)", q)
    if match:
        n, entity, metric, fy_hint = match.groups()
        fy = resolve_fy(fy_hint)
        entity_col = "Customer_Name" if "customer" in entity else entity.title()
        metric_col = "Amount" if "amount" in metric else metric.title()

        where_parts.append(f"OrderFY = '{fy}'")
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""

        return f"""
SELECT TOP {n}
    [{entity_col}],
    SUM([{metric_col}]) AS Total{metric_col}
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [{entity_col}]
ORDER BY Total{metric_col} DESC
"""

    # --------------------------------------------
    # Template 7: Count of X by Y in FY
    # --------------------------------------------
    match = re.search(r"count\s+of\s+(\w+).*?by\s+(\w+).*?fy.*?(20\d{2}-\d{2}|current|previous)", q)
    if match:
        thing, group_col, fy_hint = match.groups()
        fy = resolve_fy(fy_hint)
        thing_col = "Customer_Name" if "customer" in thing else thing.title()

        where_parts.append(f"OrderFY = '{fy}'")
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""

        return f"""
SELECT
    [{group_col}],
    COUNT(DISTINCT [{thing_col}]) AS Count_{thing_col}
FROM dbo.SalesPlanTable
{where_sql}
GROUP BY [{group_col}]
ORDER BY Count_{thing_col} DESC
"""

    # --------------------------------------------
    # Template 8: In previous month
    # --------------------------------------------
    if "previous month" in q:
        today = date.today()
        print(f"[DEBUG] Today: {today}")
        first = today.replace(day=1)
        last_month = first - timedelta(days=1)
        prev_month = last_month.strftime("%b-%y").replace(".", "")
        print(f"[DEBUG] Previous month: {prev_month}")  # ← Add this
        prev_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%b-%y").replace(".", "")
        
        # Case-insensitive match
        where_parts.append(f"UPPER([monthyear]) = UPPER('{prev_month}')")
        where_parts.append(where_extra) if where_extra else None
        where_sql = " WHERE " + " AND ".join(where_parts) if where_parts else ""

        if "count of" in q or "total amount" in q:
            return f"""
    SELECT
        [DocumentNo], [OrderDate], [Customer_Name], SUM([Amount]) AS TotalAmount
    FROM dbo.SalesPlanTable
    {where_sql}
    GROUP BY [DocumentNo], [OrderDate], [Customer_Name]
    ORDER BY TRY_CONVERT(DATE, [OrderDate]) DESC
    """
        else:
            return f"""
    SELECT
        [DocumentNo], [OrderDate], [Customer_Name], [Amount]
    FROM dbo.SalesPlanTable
    {where_sql}
    ORDER BY TRY_CONVERT(DATE, [OrderDate]) DESC
    """