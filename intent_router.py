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


def normalize_fy_quarter(text: str):
    """
    Parse fiscal year + quarter like:
    - 'FY 2024-25 Q2'
    - 'FY24 Q1'
    Returns (fy, quarter) or (None, None).
    """
    fy, quarter = None, None

    # Match full FY: FY 2024-25
    fy_match = re.search(r"fy\s*(20\d{2})[-–](\d{2})", text, re.I)
    if fy_match:
        fy = f"{fy_match.group(1)}-{fy_match.group(2)}"
    else:
        # Shorthand FY24
        short_match = re.search(r"fy\s*(\d{2})", text, re.I)
        if short_match:
            start_year = int("20" + short_match.group(1))
            fy = f"{start_year}-{(start_year+1) % 100:02d}"

    # Quarter
    q_match = re.search(r"\bQ([1-4])\b", text, re.I)
    if q_match:
        quarter = f"Q{q_match.group(1)}"

    return fy, quarter


# -------------------------------
# Filter Extraction
# -------------------------------

def extract_filters(q: str) -> List[str]:
    """
    Extract all filters from the query.
    Returns list of WHERE conditions.
    Handles:
    - FY: 'current', 'previous', '2024-25', 'FY 2024-25', 'in FY previous'
    - MFGMode
    - Customer_Name
    - Previous Month
    - Quarter
    - Month Range: "April to June"
    """
    filters = []
    q_lower = q.lower().strip()
    
    # 🔹 Special case: FY + Quarter in one phrase (e.g., "FY 2024-25 Q2")
    fyq_fy, fyq_q = normalize_fy_quarter(q)
    if fyq_fy and fyq_q:
        filters.append(f"OrderFY = '{fyq_fy}' AND OrderQuarter = '{fyq_q}'")
        return filters  # ✅ Don't let later FY/Quarter logic duplicate


    # ----------------------------------------
    # ----------------------------------------
    # 1. OrderFY: current, previous, or explicit
    # ----------------------------------------
    fy_hint = None

    # Skip if it's a "compare" query — handled by template
    if "compare" in q_lower:
        pass
    elif any(phrase in q_lower for phrase in ["current fy", "is current", "current fiscal", "this fy"]):
        fy_hint = "current"
    elif any(phrase in q_lower for phrase in ["previous fy", "last fy", "prior fy", "previous fiscal", "last fiscal"]):
        fy_hint = "previous"
    elif "fy" in q_lower:
        if "previous" in q_lower:
            fy_hint = "previous"
        elif "current" in q_lower:
            fy_hint = "current"
        else:
            fy_match = re.search(r"fy\s*[=:\s]?\s*(20\d{2}-\d{2})", q, re.I)
            if fy_match:
                fy_hint = fy_match.group(1)

    if fy_hint:
        fy = resolve_fy_hint(fy_hint)
        if fy:
            filters.append(f"OrderFY = '{fy}'")

    # ----------------------------------------
    # 2. MFGMode
    # ----------------------------------------
    mfg_match = re.search(r"mfg\s+is\s+([\w-]+)", q, re.I)
    if mfg_match:
        filters.append(f"[MFGMode] = '{mfg_match.group(1).title()}'")
        
    # Case 3: "for MFG is X"
    if "mfg is" in q_lower:
        mfg_match = re.search(r"mfg\s+is\s+([\w-]+)", q, re.I)
        if mfg_match:
            filters.append(f"[MFGMode] = '{mfg_match.group(1).title()}'")

        
    # ----------------------------------------
    # 3. Customer_Name
    # ----------------------------------------
    cust_match = None

    # Case 1: "customer is X"
    cust_match = re.search(r"customer\s+is\s+(.+?)(?:\s+(?:and|where|$)|$)", q, re.I)

    # Case 2: "for X" where X is a known customer
    if not cust_match:
        # Extract after "for" or "where"
        for_match = re.search(r"\bfor\s+(.+?)(?:\s+(?:in|by|where|$)|$)", q, re.I)
        if for_match:
            potential_cust = for_match.group(1).strip()
            # Validate against known customers? Or just trust it
            if len(potential_cust) > 3:  # Basic heuristic
                cust_match = ("", potential_cust)

    if cust_match and hasattr(cust_match, 'group'):  # If it's a regex match
        customer_value = cust_match.group(1).strip()
    elif cust_match:  # If it's a tuple from "for X"
        customer_value = cust_match[1]
    else:
        customer_value = None

    if customer_value:
        filters.append(f"[Customer_Name] = '{customer_value}'")

    # ----------------------------------------
    # 4. Previous Month
    # ----------------------------------------
    if "previous month" in q_lower:
        prev_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%b-%y")
        filters.append(f"[monthyear] = '{prev_month}'")

    # ----------------------------------------
    # 5. Quarter
    # ----------------------------------------
    q_match = re.search(r"\b(?:quarter\s+is|in|for)\s+(Q[1-4])\b", q, re.I)
    if not q_match:
        q_match = re.search(r"\bQ([1-4])\b", q, re.I)
        if q_match:
            q_val = f"Q{q_match.group(1)}"
        else:
            q_val = None
    else:
        q_val = q_match.group(1).upper()

    if q_val:
        filters.append(f"LEFT([OrderQuarter], 2) = '{q_val}'")
    
    # ----------------------------------------
    # 7. Quarter: Previous, Next, or explicit
    # ----------------------------------------
    today = datetime.now()
    current_month = today.month
    current_year = today.year

    # Figure out current fiscal quarter (Apr = Q1, Jul = Q2, Oct = Q3, Jan = Q4)
    def month_to_fq(m):
        return (m - 4) // 3 + 1  # Apr=1, May=1, Jun=1, Jul=2, ..., Jan=4

    # Current FY: e.g., 2025-26 if Apr 2025 - Mar 2026
    current_fy_start = current_year if current_month >= 4 else current_year - 1
    current_fy = f"{current_fy_start}-{str(current_year + 1)[-2:]}"

    # Current fiscal quarter
    current_fq = month_to_fq(current_month)

    # Handle "previous quarter"
    if "previous quarter" in q_lower or "last quarter" in q_lower:
        prev_fq = current_fq - 1
        prev_fy_start = current_fy_start
        if prev_fq == 0:
            prev_fq = 4
            prev_fy_start = current_fy_start - 1
        prev_fy = f"{prev_fy_start}-{str(prev_fy_start + 1)[-2:]}"
        filters.append(f"OrderFY = '{prev_fy}' AND LEFT([OrderQuarter], 2) = 'Q{prev_fq}'")

    # Handle "next quarter"
    if "next quarter" in q_lower or "coming quarter" in q_lower:
        next_fq = current_fq + 1
        next_fy_start = current_fy_start
        if next_fq == 5:
            next_fq = 1
            next_fy_start = current_fy_start + 1
        next_fy = f"{next_fy_start}-{str(next_fy_start + 1)[-2:]}"
        filters.append(f"OrderFY = '{next_fy}' AND LEFT([OrderQuarter], 2) = 'Q{next_fq}'")

    # Handle explicit "Q1", "Q2", etc. (already exists, but ensure it doesn't conflict)
    q_match = re.search(r"\b(?:quarter\s+is|in|for)\s+(Q[1-4])\b", q, re.I)
    if not q_match:
        q_match = re.search(r"\bQ([1-4])\b", q, re.I)
        if q_match:
            q_val = f"Q{q_match.group(1)}"
        else:
            q_val = None
    else:
        q_val = q_match.group(1).upper()

    if q_val:
        # Only add if not already added by "previous"/"next"
        if not any("OrderQuarter" in f for f in filters):
            filters.append(f"LEFT([OrderQuarter], 2) = '{q_val}'")
            
    # ----------------------------------------
    # 6. Month Range: "April to June", "Jan - Mar"
    # ----------------------------------------
    month_range_match = re.search(
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s*(?:to|-|–)\s*(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b",
        q, re.I
    )
    if month_range_match:
        start_raw, end_raw = month_range_match.groups()
        month_map = {
            'jan': 'Jan', 'january': 'Jan',
            'feb': 'Feb', 'february': 'Feb',
            'mar': 'Mar', 'march': 'Mar',
            'apr': 'Apr', 'april': 'Apr',
            'may': 'May',
            'jun': 'Jun', 'june': 'Jun',
            'jul': 'Jul', 'july': 'Jul',
            'aug': 'Aug', 'august': 'Aug',
            'sep': 'Sep', 'september': 'Sep',
            'oct': 'Oct', 'october': 'Oct',
            'nov': 'Nov', 'november': 'Nov',
            'dec': 'Dec', 'december': 'Dec'
        }
        start_short = month_map.get(start_raw.lower())
        end_short = month_map.get(end_raw.lower())
        if start_short and end_short:
            # Get all months in range
            month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            try:
                start_idx = month_order.index(start_short)
                end_idx = month_order.index(end_short)
                if start_idx <= end_idx:
                    months_in_range = month_order[start_idx:end_idx+1]
                    month_conditions = " OR ".join(f"LEFT([monthyear], 3) = '{m}'" for m in months_in_range)
                    filters.append(f"({month_conditions})")
            except ValueError:
                pass  # Invalid month


    # Return at the very last
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
# VER MOD 3.2 PERFECTED "by month and type", "by month, type", "by month by type", "by FY for previous"
# DO NOT CHANGE
# -------------------------------

def extract_entities(q: str) -> List[str]:
    """
    Extract all entities after 'by', handling:
    - 'by X and Y'
    - 'by X, Y'
    - 'by X by Y'
    Only stop at clause boundaries: 'for', 'in', 'where', or end.
    """
    # Match everything after the first 'by' until clause break (not 'and' or ',')
    by_match = re.search(r"by\s+(.+?)(?:\s*(?:for|in|where|$))", q, re.I)
    if not by_match:
        return []
    text = by_match.group(1).strip()

    # Split by 'and', comma, or 'by'
    parts = re.split(r"\s+and\s+|\s*,\s+|\s+by\s+", text, flags=re.I)
    entities = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        col = resolve_column(part)
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
    # 1. Compare Count: "compare Count of No in Apr-25 and May-25"
    # -------------------------------
    if intent == "compare" and "count of" in q_clean:
        # Match: "compare count of No in Apr-25 and May-25"
        count_match = re.search(r"count of ([\w\s]+?)(?:\s+(?:in|by)\s+(.+?))?\s+(?:and|vs)\s+(.+)", q_clean, re.I)
        if count_match:
            thing = count_match.group(1).strip()
            val1_raw = count_match.group(2).strip() if count_match.group(2) else count_match.group(3).split()[0]
            val2_raw = count_match.group(3).strip()

            col = resolve_column(thing) or "DocumentNo"

            # Case 1: Quarter
            q1_match = re.search(r"\bQ([1-4])\b", val1_raw, re.I)
            q2_match = re.search(r"\bQ([1-4])\b", val2_raw, re.I)
            if q1_match and q2_match:
                q1 = f"Q{q1_match.group(1)}"
                q2 = f"Q{q2_match.group(1)}"

                filters = extract_filters(q_clean)
                filters = [f for f in filters if not re.search(r"\bOrderQuarter\b", f, re.I)]

                where_sql = " WHERE " + " AND ".join(filters) if filters else ""

                return f"""
    SELECT
        COUNT(CASE WHEN LEFT([OrderQuarter], 2) = '{q1}' THEN [{col}] END) AS [{q1}],
        COUNT(CASE WHEN LEFT([OrderQuarter], 2) = '{q2}' THEN [{col}] END) AS [{q2}]
    FROM dbo.SalesPlanTable
    {where_sql}
    """

            # Case 2: Monthyear
            try:
                my1 = normalize_my(val1_raw)
                my2 = normalize_my(val2_raw)
            except:
                return None

            filters = extract_filters(q_clean)
            filters = [f for f in filters if not re.search(r"\bmonthyear\b", f, re.I)]

            where_sql = " WHERE " + " AND ".join(filters) if filters else ""

            return f"""
    SELECT
        COUNT(CASE WHEN [monthyear] = '{my1}' THEN [{col}] END) AS [{my1}],
        COUNT(CASE WHEN [monthyear] = '{my2}' THEN [{col}] END) AS [{my2}]
    FROM dbo.SalesPlanTable
    {where_sql}
    """
    # -------------------------------
    # 2. Growth: "growth between FY 2023-24 and FY 2024-25"
    # -------------------------------
    if intent == "growth":
        match = re.search(r"between\s+(.+?)\s+(?:and|to)\s+(.+)", q_clean, re.I)
        if match:
            start_raw, end_raw = match.groups()
            q_lower = q_clean.lower()

            # Case 1: FY Growth — if "year", "fy", etc. is mentioned
            if any(word in q_lower for word in ["fy", "fiscal year", "financial year", "year"]) or \
            (any(word in q_lower for word in ["previous", "current"]) and "year" in q_lower):
                # Extract FY hints
                fy1_hint = "previous" if "previous" in start_raw.lower() else start_raw.strip()
                fy2_hint = "current" if "current" in end_raw.lower() else end_raw.strip()

                fy1 = resolve_fy_hint(fy1_hint) or fy1_hint
                fy2 = resolve_fy_hint(fy2_hint) or fy2_hint

                if not re.match(r"20\d{2}-\d{2}", fy1) or not re.match(r"20\d{2}-\d{2}", fy2):
                    return None

                filters = extract_filters(q_clean)
                filters.append(f"OrderFY IN ('{fy1}', '{fy2}')")
                where_sql = " WHERE " + " AND ".join(filters) if filters else ""

                return f"""
        SELECT
            SUM(CASE WHEN OrderFY = '{fy1}' THEN Amount ELSE 0 END) AS BaseAmount,
            SUM(CASE WHEN OrderFY = '{fy2}' THEN Amount ELSE 0 END) AS NewAmount,
            (SUM(CASE WHEN OrderFY = '{fy2}' THEN Amount ELSE 0 END) - 
            SUM(CASE WHEN OrderFY = '{fy1}' THEN Amount ELSE 0 END)) AS Absolute_Growth,
            CASE 
                WHEN SUM(CASE WHEN OrderFY = '{fy1}' THEN Amount ELSE 0 END) > 0
                THEN (SUM(CASE WHEN OrderFY = '{fy2}' THEN Amount ELSE 0 END) - 
                    SUM(CASE WHEN OrderFY = '{fy1}' THEN Amount ELSE 0 END)) * 100.0 / 
                    SUM(CASE WHEN OrderFY = '{fy1}' THEN Amount ELSE 0 END)
                ELSE NULL 
            END AS Pct_Growth
        FROM dbo.SalesPlanTable
        {where_sql}
        """

            # Case 2: Month Growth (existing logic)
            def resolve_my(text: str) -> str:
                text = text.strip().lower()
                if "previous" in text:
                    prev_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%b-%y")
                    return prev_month
                elif "current" in text:
                    return date.today().strftime("%b-%y")
                else:
                    return normalize_my(text)

            start = resolve_my(start_raw)
            end = resolve_my(end_raw)

            filters = extract_filters(q_clean)
            filters.append(f"[monthyear] IN ('{start}', '{end}')")
            where_sql = " WHERE " + " AND ".join(filters)

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
    {where_sql}
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

            # ✅ Start with all filters
            filters = extract_filters(q_clean)

            # ✅ Add FY filter
            if fy_hint:
                fy = resolve_fy_hint(fy_hint)
                if fy:
                    filters.append(f"OrderFY = '{fy}'")

            # ✅ Extract all grouping entities (e.g., "by month")
            entities = extract_entities(q_clean)
            # ✅ Always include the main entity (e.g., "Items")
            if entity not in entities:
                entities = [entity] + entities

            where_sql = " WHERE " + " AND ".join(filters) if filters else ""
            select_cols = ", ".join(f"[{col}]" for col in entities)
            group_cols = ", ".join(f"[{col}]" for col in entities)

            # ✅ Debug: Move inside the block
            print(f"[DEBUG] entity_hint: {entity_hint}")
            print(f"[DEBUG] entity: {entity}")
            print(f"[DEBUG] metric_hint: {metric_hint}")
            print(f"[DEBUG] metric: {metric}")
            print(f"[DEBUG] fy_hint: {fy_hint}")
            print(f"[DEBUG] entities: {entities}")
            print(f"[DEBUG] select_cols: {select_cols}")
            print(f"[DEBUG] group_cols: {group_cols}")

            return f"""
        SELECT TOP {n}
            {select_cols}, SUM([{metric}]) AS Total{metric}
        FROM dbo.SalesPlanTable
        {where_sql}
        GROUP BY {group_cols}
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

        filters = extract_filters(q_clean)
        where_sql = " WHERE " + " AND ".join(filters) if filters else ""

        if "total amount" in q_clean:
            # Exclude 'Amount' from GROUP BY and SELECT (we're summing it)
            group_cols = ", ".join(f"[{c}]" for c in mapped_cols if c != "Amount")
            select_cols = ", ".join(f"[{c}]" for c in mapped_cols if c != "Amount")

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
            select_cols = ", ".join(f"[{c}]" for c in mapped_cols)
            return f"""
        SELECT
            {select_cols}
        FROM dbo.SalesPlanTable
        {where_sql}
        ORDER BY OrderDate DESC
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
        # Case 1: "How many unique customers in Q2 2024?"
        if "unique customers" in q_clean and re.search(r"Q([1-4])\s+(\d{4})", q_clean, re.I):
            q_match = re.search(r"Q([1-4])\s+(\d{4})", q_clean, re.I)
            if q_match:
                q_val = f"Q{q_match.group(1)}"
                year = int(q_match.group(2))

                # Map calendar year to FY: Q1-Q3 → prev year start, Q4 → same year start
                if int(q_match.group(1)) <= 3:
                    fy_start = year - 1
                else:  # Q4 = Oct-Dec → belongs to FY starting same year
                    fy_start = year
                fy = f"{fy_start}-{str(fy_start + 1)[-2:]}"

                # Use extract_filters for any extra filters (e.g., MFG, customer)
                filters = extract_filters(q_clean)
                filters.append(f"OrderFY = '{fy}'")
                filters.append(f"LEFT([OrderQuarter], 2) = '{q_val}'")

                where_sql = " WHERE " + " AND ".join(filters) if filters else ""

                return f"""
    SELECT
        COUNT(DISTINCT [Customer_Name]) AS UniqueCustomers
    FROM dbo.SalesPlanTable
    {where_sql}
    """

        # Case 2: "Count of No in apr-25" or "Count of Items"
        match = re.search(r"count of ([\w\s]+?)(?:\s+(?:by|in|where|$))", q_clean, re.I)
        if not match:
            return None

        thing = match.group(1).strip()
        col = resolve_column(thing) or "DocumentNo"

        filters = extract_filters(q_clean)

        # Extract all monthyears
        monthyears = []
        for match in re.finditer(r"(?:month\s*year\s*|my\s*)?(\w{3}-\d{2})", q_clean, re.I):
            my_val = normalize_my(match.group(1))
            if my_val not in monthyears:
                monthyears.append(my_val)

        if monthyears:
            my_list = "', '".join(monthyears)
            filters.append(f"[monthyear] IN ('{my_list}')")

        where_sql = " WHERE " + " AND ".join(filters) if filters else ""

        return f"""
    SELECT
        COUNT([{col}]) AS Count_{col}
    FROM dbo.SalesPlanTable
    {where_sql}
    """
    

    # No template matched
    return None