"""
JHW Operations Dashboard — Standalone Cron Script
Runs every 15 minutes via server cron job, queries TMW MCP, injects data
into the dashboard HTML template, and writes the result to a local directory
served by WordPress/Apache. Zero Azure dependency.
"""

import calendar
import datetime
import json
import logging
import os
import re
import urllib.error
import urllib.parse
import urllib.request

try:
    from zoneinfo import ZoneInfo
except ImportError:
    # Python < 3.9 fallback: fixed UTC-5 offset (CDT approximation for Central time)
    class ZoneInfo:  # type: ignore
        def __init__(self, key):
            self._offset = datetime.timezone(datetime.timedelta(hours=-5))
        def __call__(self, key):
            return ZoneInfo(key)
    _CENTRAL_TZ = datetime.timezone(datetime.timedelta(hours=-5))
    ZoneInfo = lambda key: _CENTRAL_TZ  # noqa: E731

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_CENTRAL = ZoneInfo("America/Chicago")

def _today_central() -> datetime.date:
    """Return the current date in US Central time."""
    return datetime.datetime.now(_CENTRAL).date()

# ── Config — set via environment variables or edit defaults below ──────────────
TMW_MCP_URL      = os.environ.get("TMW_MCP_URL",      "https://tmw.texsix.com")
TMW_MCP_KEY      = os.environ.get("TMW_MCP_KEY",      "Houston1")

# Directory where the output files are written (must be web-accessible)
# Example: /var/www/html/wp-content/uploads/jhw-dashboard
DASHBOARD_OUTPUT_DIR  = os.environ.get(
    "DASHBOARD_OUTPUT_DIR",
    "/home/jhoustonwalker/public_html/jhwopsdashboard.texsix.com",
)
# Full path to the dashboard HTML template on disk
DASHBOARD_TEMPLATE    = os.environ.get(
    "DASHBOARD_TEMPLATE",
    os.path.join(DASHBOARD_OUTPUT_DIR, "dashboard-template.html"),
)

TERMINALS = ["HOU", "LOU", "DFW", "OKC"]

_TERMINAL_OTD_NAMES = {
    "Houston":           "HOU",
    "Louisiana":         "LOU",
    "Dallas/Fort Worth": "DFW",
    "Oklahoma City":     "OKC",
}
_OTD_NAME_FILTER = "','".join(_TERMINAL_OTD_NAMES.keys())

_REV     = "(td.Linehaul_Revenue + td.Fuel_Revenue + td.Accessorial_Revenue + td.Problem_Revenue + td.Performance_Revenue)"
_NET_REV = "(td.Linehaul_Revenue + td.Accessorial_Revenue + td.Problem_Revenue + td.Performance_Revenue)"

# ── SharePoint / OXY Excel config ─────────────────────────────────────────────
# Drive & item IDs for "OXY Load Tracking.xlsx" in the oxygom SharePoint site
_OXY_SP_DRIVE_ID = "b!534TXg-WJ0CGKXA5OEiAYMHwwtdgQGBPp-GeFmngfGZmtC62hcsgQKnwzv-_v4y2"
_OXY_SP_ITEM_ID  = "01TRGF6WJXC7W64VVABBBIKS6BXQUAER6H"
# Equipment name → dashboard equip_type mapping
_OXY_EQUIP_MAP = {
    "cr00": "Oneton",  "one ton": "Oneton",  "1-ton": "Oneton",   "1ton": "Oneton",
    "id00": "Minifloat", "mini float": "Minifloat", "minifloat": "Minifloat",
    "dd00": "Tractor", "tractor": "Tractor", "semi": "Tractor",   "flatbed": "Tractor",
}


# ── TMW MCP query helper ──────────────────────────────────────────────────────

def _tmw_query(sql: str) -> list:
    url     = f"{TMW_MCP_URL.rstrip('/')}/mcp"
    headers = {
        "Content-Type": "application/json",
        "Accept":       "application/json, text/event-stream",
        "x-auth-token": TMW_MCP_KEY,
    }
    payload = {
        "jsonrpc": "2.0",
        "id":      "1",
        "method":  "tools/call",
        "params":  {"name": "run_custom_query", "arguments": {"sql_query": sql}},
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(req, timeout=60) as _r:
        raw = _r.read().decode("utf-8").strip()
    data_line = None
    for ln in raw.splitlines():
        if ln.startswith("data:"):
            data_line = ln[5:].strip()
            break
    if data_line is None:
        data_line = raw

    rpc = json.loads(data_line)
    if "error" in rpc:
        raise RuntimeError(f"MCP error: {rpc['error']}")

    text   = rpc["result"]["content"][0]["text"]
    parsed = json.loads(text)
    return parsed.get("rows", parsed) if isinstance(parsed, dict) else parsed


# ── Date helpers ──────────────────────────────────────────────────────────────

def _period_start(period: str) -> str:
    today = _today_central()
    if period == "today":
        return str(today)
    if period == "week":
        return str(today - datetime.timedelta(days=6))
    if period == "month":
        return str(today.replace(day=1))
    raise ValueError(f"Unknown period: {period}")


def _period_label(period: str) -> str:
    today = _today_central()
    if period == "today":
        return f"Today ({today.strftime('%b %-d')})"
    if period == "week":
        seven_ago = today - datetime.timedelta(days=6)
        return f"Last 7 Days ({seven_ago.strftime('%b %-d')}-{today.strftime('%-d')})"
    if period == "month":
        return f"Month to Date ({today.strftime('%b 1-%-d')})"
    return period


# ── TMW data queries ──────────────────────────────────────────────────────────

def _query_terminals(start: str) -> dict:
    sql = f"""
SELECT
    rt.Revtype_1Name              AS terminal,
    SUM({_REV})                   AS revenue,
    SUM({_NET_REV})               AS net_revenue,
    COUNT(DISTINCT td.TripOrder_Key) AS loads,
    SUM(td.Loaded_Miles)          AS loaded_miles,
    SUM(td.Empty_Miles)           AS empty_miles
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension  rt ON td.RevType_Key    = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwTripOrderDimension o  ON td.TripOrder_key = o.TripOrder_Key
JOIN TMW_Live.dbo.orderheader oh ON TRY_CAST(RTRIM(oh.ord_number) AS int) = o.TripOrder_OrderNumber
OUTER APPLY (
    SELECT TOP 1
        CAST(s.stp_arrivaldate   AS DATE) AS billable_arr_date,
        CAST(s.stp_schdtearliest AS DATE) AS billable_sched_date
    FROM TMW_Live.dbo.stops s
    WHERE s.ord_hdrnumber = oh.ord_hdrnumber
      AND s.stp_event NOT IN ('BMT','EMT','BBT','EBT')
    ORDER BY s.stp_sequence DESC
) lbs
WHERE ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) >= '{start}'
  AND ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) <= CAST(GETDATE() AS DATE)
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
GROUP BY rt.Revtype_1Name
"""
    rows   = _tmw_query(sql)
    result = {t: {"revenue": 0, "net_revenue": 0, "loads": 0, "loaded_miles": 0, "empty_miles": 0} for t in TERMINALS}
    for row in rows:
        t = _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), "")
        if t in result:
            result[t] = {
                "revenue":      round(float(row.get("revenue")      or 0), 2),
                "net_revenue":  round(float(row.get("net_revenue")  or 0), 2),
                "loads":        int(row.get("loads")        or 0),
                "loaded_miles": int(row.get("loaded_miles") or 0),
                "empty_miles":  int(row.get("empty_miles")  or 0),
            }
    return result


def _rows_to_customers(rows: list) -> list:
    result = []
    for row in rows:
        rev   = round(float(row.get("revenue") or 0), 2)
        loads = int(row.get("loads") or 1)
        result.append({
            "name":       row.get("name", "Unknown"),
            "revenue":    rev,
            "loads":      loads,
            "avgPerLoad": round(rev / loads, 2) if loads else 0,
        })
    return result


def _query_customers(start: str) -> list:
    sql = f"""
SELECT TOP 10
    c.Company_Name                   AS name,
    SUM({_REV})                      AS revenue,
    COUNT(DISTINCT td.TripOrder_Key) AS loads
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension  rt ON td.RevType_Key    = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwTripOrderDimension o  ON td.TripOrder_key = o.TripOrder_Key
JOIN tmw_dwlive.dbo.dwCompanyDimension   c  ON td.Customer_Key  = c.Company_Key
JOIN TMW_Live.dbo.orderheader oh ON TRY_CAST(RTRIM(oh.ord_number) AS int) = o.TripOrder_OrderNumber
OUTER APPLY (
    SELECT TOP 1
        CAST(s.stp_arrivaldate   AS DATE) AS billable_arr_date,
        CAST(s.stp_schdtearliest AS DATE) AS billable_sched_date
    FROM TMW_Live.dbo.stops s
    WHERE s.ord_hdrnumber = oh.ord_hdrnumber
      AND s.stp_event NOT IN ('BMT','EMT','BBT','EBT')
    ORDER BY s.stp_sequence DESC
) lbs
WHERE ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) >= '{start}'
  AND ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) <= CAST(GETDATE() AS DATE)
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
GROUP BY c.Company_Name
ORDER BY revenue DESC
"""
    return _rows_to_customers(_tmw_query(sql))


def _query_terminal_customers(start: str) -> dict:
    sql = f"""
WITH base AS (
    SELECT
        rt.Revtype_1Name                 AS terminal,
        c.Company_Name                   AS name,
        SUM({_REV})                      AS revenue,
        COUNT(DISTINCT td.TripOrder_Key) AS loads
    FROM tmw_dwlive.dbo.dwFacts_TripDetail td
    JOIN tmw_dwlive.dbo.dwRevTypeDimension  rt ON td.RevType_Key    = rt.RevType_Key
    JOIN tmw_dwlive.dbo.dwTripOrderDimension o  ON td.TripOrder_key = o.TripOrder_Key
    JOIN tmw_dwlive.dbo.dwCompanyDimension   c  ON td.Customer_Key  = c.Company_Key
    JOIN TMW_Live.dbo.orderheader oh ON TRY_CAST(RTRIM(oh.ord_number) AS int) = o.TripOrder_OrderNumber
    OUTER APPLY (
        SELECT TOP 1
            CAST(s.stp_arrivaldate   AS DATE) AS billable_arr_date,
            CAST(s.stp_schdtearliest AS DATE) AS billable_sched_date
        FROM TMW_Live.dbo.stops s
        WHERE s.ord_hdrnumber = oh.ord_hdrnumber
          AND s.stp_event NOT IN ('BMT','EMT','BBT','EBT')
        ORDER BY s.stp_sequence DESC
    ) lbs
    WHERE ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) >= '{start}'
      AND ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) <= CAST(GETDATE() AS DATE)
      AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
    GROUP BY rt.Revtype_1Name, c.Company_Name
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY terminal ORDER BY revenue DESC) AS rn
    FROM base
)
SELECT terminal, name, revenue, loads
FROM ranked
WHERE rn <= 10
ORDER BY terminal, rn
"""
    rows   = _tmw_query(sql)
    result = {t: [] for t in TERMINALS}
    for row in rows:
        t = _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), "")
        if t in result:
            rev   = round(float(row.get("revenue") or 0), 2)
            loads = int(row.get("loads") or 1)
            result[t].append({
                "name":       row.get("name", "Unknown"),
                "revenue":    rev,
                "loads":      loads,
                "avgPerLoad": round(rev / loads, 2) if loads else 0,
            })
    return result


def _query_drivers(start: str) -> list:
    sql = f"""
WITH driver_trips AS (
    SELECT
        td.Driver_Key  AS drv_key,
        rt.Revtype_1Name AS terminal,
        td.TripOrder_Key,
        CASE
            WHEN ISNULL(d2.Driver_Firstname,'') NOT IN ('Unknown','')
            THEN {_REV} / 2.0
            ELSE {_REV}
        END AS revenue,
        td.Pay_Cost AS pay
    FROM tmw_dwlive.dbo.dwFacts_TripDetail   td
    JOIN tmw_dwlive.dbo.dwRevTypeDimension   rt ON td.RevType_Key        = rt.RevType_Key
    JOIN tmw_dwlive.dbo.dwDateDimension      dd ON td.OrderEndDate_Key = dd.Date_Key
    JOIN tmw_dwlive.dbo.dwTripOrderDimension o  ON td.TripOrder_Key      = o.TripOrder_Key
    JOIN TMW_Live.dbo.orderheader            oh ON TRY_CAST(RTRIM(oh.ord_number) AS int) = o.TripOrder_OrderNumber
    OUTER APPLY (
        -- Last billable stop: actual arrival if complete, else expected arrival (stp_schdtearliest)
        SELECT TOP 1
            CAST(s.stp_arrivaldate   AS DATE) AS billable_arr_date,
            CAST(s.stp_schdtearliest AS DATE) AS billable_sched_date
        FROM TMW_Live.dbo.stops s
        WHERE s.ord_hdrnumber = oh.ord_hdrnumber
          AND s.stp_event NOT IN ('BMT','EMT','BBT','EBT')
        ORDER BY s.stp_sequence DESC
    ) lbs
    LEFT JOIN tmw_dwlive.dbo.dwDriverDimension d2
        ON  td.Driver2_Key > 0
        AND td.Driver2_Key = d2.Driver_Key
        AND ISNULL(d2.Driver_Firstname,'') NOT IN ('Unknown','NotDefined','')
    WHERE ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) >= '{start}'
      AND ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) <= CAST(GETDATE() AS DATE)
      AND dd.Date_Key < 50000
      AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')

    UNION ALL
    SELECT
        td.Driver2_Key AS drv_key,
        rt.Revtype_1Name AS terminal,
        td.TripOrder_Key,
        {_REV} / 2.0 AS revenue,
        NULL          AS pay
    FROM tmw_dwlive.dbo.dwFacts_TripDetail   td
    JOIN tmw_dwlive.dbo.dwRevTypeDimension   rt ON td.RevType_Key        = rt.RevType_Key
    JOIN tmw_dwlive.dbo.dwDateDimension      dd ON td.OrderEndDate_Key = dd.Date_Key
    JOIN tmw_dwlive.dbo.dwTripOrderDimension o  ON td.TripOrder_Key      = o.TripOrder_Key
    JOIN TMW_Live.dbo.orderheader            oh ON TRY_CAST(RTRIM(oh.ord_number) AS int) = o.TripOrder_OrderNumber
    OUTER APPLY (
        -- Last billable stop: actual arrival if complete, else expected arrival (stp_schdtearliest)
        SELECT TOP 1
            CAST(s.stp_arrivaldate   AS DATE) AS billable_arr_date,
            CAST(s.stp_schdtearliest AS DATE) AS billable_sched_date
        FROM TMW_Live.dbo.stops s
        WHERE s.ord_hdrnumber = oh.ord_hdrnumber
          AND s.stp_event NOT IN ('BMT','EMT','BBT','EBT')
        ORDER BY s.stp_sequence DESC
    ) lbs
    JOIN tmw_dwlive.dbo.dwDriverDimension    d2 ON td.Driver2_Key        = d2.Driver_Key
    WHERE ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) >= '{start}'
      AND ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) <= CAST(GETDATE() AS DATE)
      AND dd.Date_Key < 50000
      AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
      AND td.Driver2_Key > 0
      AND ISNULL(d2.Driver_Firstname,'') NOT IN ('Unknown','NotDefined','')
)
SELECT TOP 15
    d.Driver_Firstname + ' ' + d.Driver_Lastname AS name,
    dt.terminal,
    COUNT(DISTINCT dt.TripOrder_Key)             AS loads,
    SUM(dt.revenue)                              AS revenue,
    SUM(dt.pay)                                  AS pay,
    d.Driver_Type1                               AS driver_type
FROM driver_trips dt
JOIN tmw_dwlive.dbo.dwDriverDimension d ON dt.drv_key = d.Driver_Key
WHERE ISNULL(d.Driver_Firstname,'') NOT IN ('Unknown','NotDefined','BrokerLoad','')
  AND ISNULL(d.Driver_Lastname,'')  NOT IN ('NotDefined','BrokerLoad','')
  AND d.Driver_Type1 != 'BrokerLoad'
  AND d.Driver_Key > 2
GROUP BY d.Driver_Firstname, d.Driver_Lastname, dt.terminal, d.Driver_Type1
ORDER BY revenue DESC
"""
    rows   = _tmw_query(sql)
    result = []
    for row in rows:
        rev     = round(float(row.get("revenue") or 0), 2)
        raw_pay = row.get("pay")
        pay     = round(float(raw_pay), 2) if raw_pay and float(raw_pay) > 0 else None
        dtype   = (row.get("driver_type") or "").upper()
        if "OO" in dtype or "OWNER" in dtype:
            dtype = "OO"
        elif "DFO" in dtype or "LESSEE" in dtype:
            dtype = "DFO"
        else:
            dtype = "Company"
        result.append({
            "name":     row.get("name", "Unknown"),
            "terminal": _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), "HOU"),
            "loads":    int(row.get("loads") or 0),
            "revenue":  rev,
            "pay":      pay,
            "type":     dtype,
        })
    return result


def _query_otd(start: str) -> dict:
    # Detect the latest date the DW actually has data for (ETL typically lags 1-3 days)
    sql_dw_max = f"""
SELECT MAX(dd.Date_Date) AS dw_max_date
FROM tmw_dwlive.dbo.dwFacts_StopDetail sd
JOIN tmw_dwlive.dbo.dwDateDimension dd ON sd.StopScheduledLatestDate_key = dd.Date_Key
JOIN tmw_dwlive.dbo.dwRevTypeDimension rt ON sd.Revtype_key = rt.RevType_Key
WHERE dd.Date_Date >= '2025-01-01'
  AND dd.Date_Date <= CAST(GETDATE() AS DATE)
  AND sd.Arrive_MinsLate IS NOT NULL
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
"""
    try:
        max_rows = _tmw_query(sql_dw_max)
        dw_max = str(max_rows[0].get("dw_max_date", "") or "")[:10] if max_rows else ""
    except Exception:
        dw_max = ""

    # If DW hasn't caught up to the requested start (e.g. "today"), use dw_max as start
    effective_start = dw_max if (dw_max and start > dw_max) else start
    # Use dw_max + 1 day as upper bound so we never query beyond available data
    ceiling = f"DATEADD(DAY, 1, CAST('{dw_max}' AS DATE))" if dw_max else "DATEADD(DAY, 1, CAST(GETDATE() AS DATE))"

    sql_summary = f"""
SELECT
    rt.Revtype_1Name                                                                  AS terminal,
    COUNT(*)                                                                          AS total_stops,
    SUM(CASE WHEN sd.Arrive_MinsLate = 0 THEN 1 ELSE 0 END)                         AS on_time,
    SUM(CASE WHEN sd.Arrive_MinsLate > 0 THEN 1 ELSE 0 END)                         AS late,
    SUM(CASE WHEN sd.Arrive_MinsLate > 0
             AND  sd.LateArriveBridgeStop_Key = -1 THEN 1 ELSE 0 END)               AS late_no_reason,
    ROUND(AVG(CASE WHEN sd.Arrive_MinsLate > 0
                    AND sd.Arrive_MinsLate <= 1440
                   THEN CAST(sd.Arrive_MinsLate AS float) END), 0)                   AS avg_mins_late
FROM tmw_dwlive.dbo.dwFacts_StopDetail sd
JOIN tmw_dwlive.dbo.dwRevTypeDimension      rt  ON sd.Revtype_key                 = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension         dd  ON sd.StopScheduledLatestDate_key = dd.Date_Key
JOIN tmw_dwlive.dbo.dwStopElementsDimension se  ON sd.StopElements_key            = se.StopElements_Key
WHERE dd.Date_Date >= '{effective_start}'
  AND dd.Date_Date <  {ceiling}
  AND dd.Date_Date >= '2020-01-01'
  AND sd.Arrive_MinsLate IS NOT NULL
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
  AND se.StopElements_EventFreightActivity IN ('PUP','DRP')
  AND se.StopElements_Status = 'Done'
  AND sd.StopScheduledLatestTime_key != 1440
GROUP BY rt.Revtype_1Name
"""
    sql_reasons = f"""
SELECT
    CASE
        WHEN sd.LateArriveBridgeStop_Key = -1  THEN 'No Reason Code'
        WHEN lb.Label_Name = 'NotLocated'       THEN 'No Reason Code'
        ELSE ISNULL(lb.Label_Name, 'No Reason Code')
    END                                         AS reason,
    COUNT(*)                                    AS late_count
FROM tmw_dwlive.dbo.dwFacts_StopDetail sd
JOIN tmw_dwlive.dbo.dwRevTypeDimension      rt  ON sd.Revtype_key                 = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension         dd  ON sd.StopScheduledLatestDate_key = dd.Date_Key
JOIN tmw_dwlive.dbo.dwStopElementsDimension se  ON sd.StopElements_key            = se.StopElements_Key
LEFT JOIN tmw_dwlive.dbo.dwLateArriveBridgeStop lab
       ON sd.LateArriveBridgeStop_Key = lab.LateArriveBridgeStop_Key
      AND sd.LateArriveBridgeStop_Key != -1
LEFT JOIN tmw_dwlive.dbo.dwLabelDimension lb ON lab.Label_Key = lb.Label_Key
WHERE dd.Date_Date >= '{effective_start}'
  AND dd.Date_Date <  {ceiling}
  AND dd.Date_Date >= '2020-01-01'
  AND sd.Arrive_MinsLate > 0
  AND sd.Arrive_MinsLate IS NOT NULL
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
  AND se.StopElements_EventFreightActivity IN ('PUP','DRP')
  AND se.StopElements_Status = 'Done'
  AND sd.StopScheduledLatestTime_key != 1440
GROUP BY
    CASE
        WHEN sd.LateArriveBridgeStop_Key = -1  THEN 'No Reason Code'
        WHEN lb.Label_Name = 'NotLocated'       THEN 'No Reason Code'
        ELSE ISNULL(lb.Label_Name, 'No Reason Code')
    END
ORDER BY late_count DESC
"""
    try:
        summary_rows = _tmw_query(sql_summary)
    except Exception as e:
        logging.warning(f"OTD summary query failed: {e}")
        summary_rows = []
    try:
        reason_rows = _tmw_query(sql_reasons)
    except Exception as e:
        logging.warning(f"OTD reasons query failed: {e}")
        reason_rows = []

    by_terminal = {}
    agg = {"total": 0, "on_time": 0, "late": 0, "late_no_reason": 0}
    for row in summary_rows:
        code = _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), "")
        if not code:
            continue
        total  = int(row.get("total_stops")    or 0)
        on_t   = int(row.get("on_time")        or 0)
        late   = int(row.get("late")           or 0)
        no_rsn = int(row.get("late_no_reason") or 0)
        avg_l  = row.get("avg_mins_late")
        avg_l  = int(float(avg_l)) if avg_l else None
        by_terminal[code] = {
            "total": total, "on_time": on_t, "late": late,
            "late_no_reason": no_rsn, "avg_mins_late": avg_l,
            "otd_pct": round(on_t / total * 100, 1) if total else None,
        }
        agg["total"] += total; agg["on_time"] += on_t
        agg["late"]  += late;  agg["late_no_reason"] += no_rsn

    by_terminal["all"] = {
        "total": agg["total"], "on_time": agg["on_time"], "late": agg["late"],
        "late_no_reason": agg["late_no_reason"], "avg_mins_late": None,
        "otd_pct": round(agg["on_time"] / agg["total"] * 100, 1) if agg["total"] else None,
    }
    reasons = [
        {"reason": r.get("reason", "Unknown"), "count": int(r.get("late_count") or 0)}
        for r in reason_rows
    ]
    return {"by_terminal": by_terminal, "late_reasons": reasons,
            "total_late_no_reason": agg["late_no_reason"],
            "dw_date": dw_max}


def _query_trends() -> dict:
    """Query revenue trend data for the new two-level chart design.
    Returns historical completed months, current month MTD/projected/LY-same-period,
    and YTD actual/projected/LY-same-period — all broken down by terminal.
    """
    import calendar as _cal
    today          = _today_central()
    curr_yr        = today.year
    curr_mo        = today.month
    days_elapsed   = today.day
    days_in_month  = _cal.monthrange(curr_yr, curr_mo)[1]
    curr_month_start = today.replace(day=1)

    # Last-year equivalents
    ly_yr          = curr_yr - 1
    ly_month_start = curr_month_start.replace(year=ly_yr)
    ly_day         = min(days_elapsed, _cal.monthrange(ly_yr, curr_mo)[1])
    import datetime as _dt
    ly_same_day    = _dt.date(ly_yr, curr_mo, ly_day)
    ytd_start      = today.replace(month=1, day=1)
    ly_ytd_start   = ytd_start.replace(year=ly_yr)
    # LY YTD end = same calendar position last year
    ly_ytd_mo_day  = min(today.day, _cal.monthrange(ly_yr, curr_mo)[1])
    ly_ytd_end     = _dt.date(ly_yr, curr_mo, ly_ytd_mo_day)

    month_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    # ── 1. Completed months (24 months back, excluding current month) ──────────
    sql_history = f"""
SELECT
    YEAR(dd.Date_Date)               AS yr,
    MONTH(dd.Date_Date)              AS mo,
    rt.Revtype_1Name                 AS terminal,
    SUM({_REV})                      AS revenue
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension rt ON td.RevType_Key        = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension    dd ON td.OrderEndDate_Key = dd.Date_Key
WHERE dd.Date_Date >= DATEADD(MONTH, -24, DATEFROMPARTS({curr_yr}, {curr_mo}, 1))
  AND dd.Date_Date <  DATEFROMPARTS({curr_yr}, {curr_mo}, 1)
  AND dd.Date_Key < 50000
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
GROUP BY YEAR(dd.Date_Date), MONTH(dd.Date_Date), rt.Revtype_1Name
ORDER BY yr, mo
"""

    # ── 2. Current month MTD ───────────────────────────────────────────────────
    sql_mtd = f"""
SELECT rt.Revtype_1Name AS terminal, SUM({_REV}) AS revenue
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension rt ON td.RevType_Key        = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension    dd ON td.OrderEndDate_Key = dd.Date_Key
WHERE dd.Date_Date >= '{curr_month_start}'
  AND dd.Date_Date <= CAST(GETDATE() AS DATE)
  AND dd.Date_Key < 50000
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
GROUP BY rt.Revtype_1Name
"""

    # ── 3. LY same period (same month, same days 1–N last year) ───────────────
    sql_ly_mtd = f"""
SELECT rt.Revtype_1Name AS terminal, SUM({_REV}) AS revenue
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension rt ON td.RevType_Key        = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension    dd ON td.OrderEndDate_Key = dd.Date_Key
WHERE dd.Date_Date >= '{ly_month_start}'
  AND dd.Date_Date <= '{ly_same_day}'
  AND dd.Date_Key < 50000
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
GROUP BY rt.Revtype_1Name
"""

    # ── 4. YTD actual (Jan 1 → today) ─────────────────────────────────────────
    sql_ytd = f"""
SELECT rt.Revtype_1Name AS terminal, SUM({_REV}) AS revenue
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension rt ON td.RevType_Key        = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension    dd ON td.OrderEndDate_Key = dd.Date_Key
WHERE dd.Date_Date >= '{ytd_start}'
  AND dd.Date_Date <= CAST(GETDATE() AS DATE)
  AND dd.Date_Key < 50000
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
GROUP BY rt.Revtype_1Name
"""

    # ── 5. LY YTD same period ──────────────────────────────────────────────────
    sql_ly_ytd = f"""
SELECT rt.Revtype_1Name AS terminal, SUM({_REV}) AS revenue
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension rt ON td.RevType_Key        = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension    dd ON td.OrderEndDate_Key = dd.Date_Key
WHERE dd.Date_Date >= '{ly_ytd_start}'
  AND dd.Date_Date <= '{ly_ytd_end}'
  AND dd.Date_Key < 50000
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
GROUP BY rt.Revtype_1Name
"""

    try:
        history_rows = _tmw_query(sql_history)
        mtd_rows     = _tmw_query(sql_mtd)
        ly_mtd_rows  = _tmw_query(sql_ly_mtd)
        ytd_rows     = _tmw_query(sql_ytd)
        ly_ytd_rows  = _tmw_query(sql_ly_ytd)
    except Exception as e:
        logging.warning(f"Trend query failed: {e}")
        return {}

    # ── Build history list ─────────────────────────────────────────────────────
    month_map = {}
    for row in history_rows:
        yr = int(row["yr"]); mo = int(row["mo"]); key = (yr, mo)
        if key not in month_map:
            month_map[key] = {"yr": yr, "mo": mo}
        t = _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), "")
        if t:
            month_map[key][t] = round(float(row.get("revenue") or 0), 2)

    history = []
    for (yr, mo), entry in sorted(month_map.items()):
        entry["label"] = f"{month_names[mo-1]} '{str(yr)[2:]}"
        entry["total"] = round(sum(entry.get(t, 0) for t in TERMINALS), 2)
        for t in TERMINALS:
            entry.setdefault(t, 0.0)
        history.append(entry)

    # ── Helper: rows → terminal dict with total ────────────────────────────────
    def _tdict(rows):
        d = {t: 0.0 for t in TERMINALS}
        for row in rows:
            t = _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), "")
            if t in d:
                d[t] = round(float(row.get("revenue") or 0), 2)
        d["total"] = round(sum(d[t] for t in TERMINALS), 2)
        return d

    mtd        = _tdict(mtd_rows)
    ly_mtd     = _tdict(ly_mtd_rows)
    ytd_actual = _tdict(ytd_rows)
    ly_ytd     = _tdict(ly_ytd_rows)

    # ── Projections ────────────────────────────────────────────────────────────
    projected = {}
    for t in TERMINALS:
        projected[t] = round(mtd[t] / days_elapsed * days_in_month, 2) if days_elapsed > 0 and mtd[t] > 0 else 0.0
    projected["total"] = round(sum(projected[t] for t in TERMINALS), 2)

    # YTD projected = YTD actual + remaining days at current run rate
    ytd_projected = {}
    for t in TERMINALS:
        ytd_projected[t] = round(ytd_actual[t] + max(0.0, projected[t] - mtd[t]), 2)
    ytd_projected["total"] = round(sum(ytd_projected[t] for t in TERMINALS), 2)

    # ── Last complete month metadata ───────────────────────────────────────────
    lc_mo = curr_mo - 1 if curr_mo > 1 else 12
    lc_yr = curr_yr     if curr_mo > 1 else curr_yr - 1
    last_complete = next((e for e in reversed(history) if e["yr"] == lc_yr and e["mo"] == lc_mo), history[-1] if history else None)
    ly_last_complete = next((e for e in history if e["yr"] == lc_yr - 1 and e["mo"] == lc_mo), None)

    # ── Best completed month so far this calendar year ─────────────────────────
    cy_months = [e for e in history if e["yr"] == curr_yr]
    best_month = max(cy_months, key=lambda e: e["total"]) if cy_months else None

    return {
        "history": history,
        "current": {
            "yr":           curr_yr,
            "mo":           curr_mo,
            "label":        f"{month_names[curr_mo-1]} {curr_yr}",
            "days_elapsed": days_elapsed,
            "days_in_month":days_in_month,
            "mtd":          mtd,
            "projected":    projected,
            "ly_same_period": ly_mtd,
        },
        "ytd": {
            "actual":         ytd_actual,
            "projected":      ytd_projected,
            "ly_same_period": ly_ytd,
            "last_complete": {
                "label":    last_complete["label"]    if last_complete    else "",
                "total":    last_complete["total"]    if last_complete    else 0,
                "ly_total": ly_last_complete["total"] if ly_last_complete else 0,
            },
            "best_month": {
                "label": best_month["label"] if best_month else "",
                "total": best_month["total"] if best_month else 0,
            } if best_month else None,
        },
    }


def _query_driver_loads(start: str) -> dict:
    """Return per-load detail for each driver — powers the drill-down modal.
    Revenue attributed to actual completion date for closed loads,
    scheduled final date for still-open loads."""
    sql = f"""
SELECT
    d.Driver_Firstname + ' ' + d.Driver_Lastname      AS driver_name,
    CONVERT(varchar(10),
        ISNULL(lbs.billable_arr_date, lbs.billable_sched_date), 23) AS load_date,
    o.TripOrder_OrderNumber                           AS order_num,
    c.Company_Name                                    AS customer,
    rt.Revtype_1Name                                  AS terminal,
    {_REV}                                            AS revenue,
    td.Pay_Cost                                       AS pay,
    td.Loaded_Miles                                   AS loaded_miles,
    td.Empty_Miles                                    AS empty_miles
FROM tmw_dwlive.dbo.dwFacts_TripDetail   td
JOIN tmw_dwlive.dbo.dwDriverDimension    d  ON td.Driver_Key         = d.Driver_Key
JOIN tmw_dwlive.dbo.dwRevTypeDimension   rt ON td.RevType_Key        = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension      dd ON td.OrderEndDate_Key = dd.Date_Key
JOIN tmw_dwlive.dbo.dwCompanyDimension   c  ON td.Customer_Key       = c.Company_Key
JOIN tmw_dwlive.dbo.dwTripOrderDimension o  ON td.TripOrder_Key      = o.TripOrder_Key
JOIN TMW_Live.dbo.orderheader            oh ON TRY_CAST(RTRIM(oh.ord_number) AS int) = o.TripOrder_OrderNumber
OUTER APPLY (
    SELECT TOP 1
        CAST(s.stp_arrivaldate   AS DATE) AS billable_arr_date,
        CAST(s.stp_schdtearliest AS DATE) AS billable_sched_date
    FROM TMW_Live.dbo.stops s
    WHERE s.ord_hdrnumber = oh.ord_hdrnumber
      AND s.stp_event NOT IN ('BMT','EMT','BBT','EBT')
    ORDER BY s.stp_sequence DESC
) lbs
WHERE ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) >= '{start}'
  AND ISNULL(lbs.billable_arr_date, lbs.billable_sched_date) <= CAST(GETDATE() AS DATE)
  AND dd.Date_Key < 50000
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
  AND ISNULL(d.Driver_Firstname,'') NOT IN ('Unknown','NotDefined','BrokerLoad','')
  AND ISNULL(d.Driver_Lastname,'')  NOT IN ('NotDefined','BrokerLoad','')
  AND d.Driver_Type1 != 'BrokerLoad'
  AND d.Driver_Key > 2
ORDER BY d.Driver_Firstname, d.Driver_Lastname, load_date DESC
"""
    rows   = _tmw_query(sql)
    result: dict = {}
    for row in rows:
        name = row.get("driver_name", "Unknown")
        rev  = round(float(row.get("revenue") or 0), 2)
        raw_pay = row.get("pay")
        pay  = round(float(raw_pay), 2) if raw_pay and float(raw_pay) > 0 else None
        result.setdefault(name, []).append({
            "date":         row.get("load_date", ""),
            "order":        str(row.get("order_num") or ""),
            "customer":     row.get("customer", ""),
            "terminal":     _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), ""),
            "revenue":      rev,
            "pay":          pay,
            "loaded_miles": int(row.get("loaded_miles") or 0),
            "empty_miles":  int(row.get("empty_miles")  or 0),
        })
    return result


def _query_customer_loads(start: str) -> dict:
    """Return per-load detail for each customer — powers the customer drill-down modal."""
    sql = f"""
SELECT
    c.Company_Name                                    AS customer,
    CONVERT(varchar(10), dd.Date_Date, 23)            AS load_date,
    o.TripOrder_OrderNumber                           AS order_num,
    d.Driver_Firstname + ' ' + d.Driver_Lastname      AS driver_name,
    rt.Revtype_1Name                                  AS terminal,
    {_REV}                                            AS revenue,
    td.Loaded_Miles                                   AS loaded_miles,
    td.Empty_Miles                                    AS empty_miles
FROM tmw_dwlive.dbo.dwFacts_TripDetail   td
JOIN tmw_dwlive.dbo.dwCompanyDimension   c  ON td.Customer_Key      = c.Company_Key
JOIN tmw_dwlive.dbo.dwRevTypeDimension   rt ON td.RevType_Key       = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension      dd ON td.OrderEndDate_Key = dd.Date_Key
JOIN tmw_dwlive.dbo.dwDriverDimension    d  ON td.Driver_Key        = d.Driver_Key
JOIN tmw_dwlive.dbo.dwTripOrderDimension o  ON td.TripOrder_Key     = o.TripOrder_Key
WHERE dd.Date_Date >= '{start}'
  AND dd.Date_Date <= CAST(GETDATE() AS DATE)
  AND dd.Date_Key < 50000
  AND rt.Revtype_1Name IN ('{_OTD_NAME_FILTER}')
ORDER BY c.Company_Name, dd.Date_Date DESC
"""
    rows   = _tmw_query(sql)
    result: dict = {}
    for row in rows:
        name = row.get("customer", "Unknown")
        rev  = round(float(row.get("revenue") or 0), 2)
        result.setdefault(name, []).append({
            "date":         row.get("load_date", ""),
            "order":        str(row.get("order_num") or ""),
            "driver":       row.get("driver_name", ""),
            "terminal":     _TERMINAL_OTD_NAMES.get(row.get("terminal", ""), ""),
            "revenue":      rev,
            "loaded_miles": int(row.get("loaded_miles") or 0),
            "empty_miles":  int(row.get("empty_miles")  or 0),
        })
    return result


def _query_brokered_loads(start: str) -> dict:
    """Query brokered/outside-carrier loads.
    Brokered loads are identified by Driver_Key = 2 (the TMW BrokerLoad placeholder).
    Returns carrier-level summaries plus individual load detail for drill-down.
    Cross-referenced against live order board — Driver_Key = 2 captures all brokered loads.
    """
    # ── Customer-level summary ─────────────────────────────────────────────────
    sql_summary = f"""
SELECT
    c.Company_Name                   AS customer,
    COUNT(DISTINCT td.TripOrder_Key) AS loads,
    SUM({_REV})                      AS revenue,
    SUM(td.Pay_Cost)                 AS carrier_cost
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwDateDimension    dd  ON td.OrderEndDate_Key = dd.Date_Key
JOIN tmw_dwlive.dbo.dwCompanyDimension c   ON td.Customer_Key       = c.Company_Key
WHERE td.Driver_Key = 2
  AND dd.Date_Date >= '{start}'
  AND dd.Date_Date <= CAST(GETDATE() AS DATE)
  AND dd.Date_Key < 50000
GROUP BY c.Company_Name
ORDER BY revenue DESC
"""
    # ── Individual load detail (for drill-down modal) ─────────────────────────
    sql_loads = f"""
SELECT TOP 500
    dd.Date_Date                      AS order_date,
    tod.TripOrder_OrderNumber         AS order_num,
    c.Company_Name                    AS customer,
    car.Carrier_Name                  AS carrier,
    rt.Revtype_1Name                  AS terminal,
    {_REV}                            AS revenue,
    td.Pay_Cost                       AS carrier_cost
FROM tmw_dwlive.dbo.dwFacts_TripDetail td
JOIN tmw_dwlive.dbo.dwRevTypeDimension   rt  ON td.RevType_Key        = rt.RevType_Key
JOIN tmw_dwlive.dbo.dwDateDimension      dd  ON td.OrderEndDate_Key = dd.Date_Key
JOIN tmw_dwlive.dbo.dwCompanyDimension   c   ON td.Customer_Key       = c.Company_Key
JOIN tmw_dwlive.dbo.dwCarrierDimension   car ON td.Carrier_Key        = car.Carrier_Key
JOIN tmw_dwlive.dbo.dwTripOrderDimension tod ON td.TripOrder_Key      = tod.TripOrder_Key
WHERE td.Driver_Key = 2
  AND dd.Date_Date >= '{start}'
  AND dd.Date_Date <= CAST(GETDATE() AS DATE)
  AND dd.Date_Key < 50000
ORDER BY dd.Date_Date DESC, tod.TripOrder_OrderNumber DESC
"""
    try:
        summary_rows = _tmw_query(sql_summary)
        load_rows    = _tmw_query(sql_loads)
    except Exception as e:
        logging.warning(f"Brokered loads query failed: {e}")
        return {}

    # ── Build customer summaries ───────────────────────────────────────────────
    customers   = []
    total_loads = 0
    total_rev   = 0.0
    total_cost  = 0.0
    for row in summary_rows:
        rev    = round(float(row.get("revenue")      or 0), 2)
        cost   = round(float(row.get("carrier_cost") or 0), 2)
        profit = round(rev - cost, 2)
        margin = round(profit / rev * 100, 1) if rev else 0.0
        loads  = int(row.get("loads") or 0)
        customers.append({
            "customer":     row.get("customer", "Unknown"),
            "loads":        loads,
            "revenue":      rev,
            "carrier_cost": cost,
            "profit":       profit,
            "margin_pct":   margin,
        })
        total_loads += loads
        total_rev   += rev
        total_cost  += cost

    total_profit = round(total_rev - total_cost, 2)
    total_margin = round(total_profit / total_rev * 100, 1) if total_rev else 0.0

    # ── Build individual loads list ────────────────────────────────────────────
    loads_list = []
    for row in load_rows:
        rev    = round(float(row.get("revenue")      or 0), 2)
        cost   = round(float(row.get("carrier_cost") or 0), 2)
        profit = round(rev - cost, 2)
        margin = round(profit / rev * 100, 1) if rev else 0.0
        t_full = row.get("terminal", "")
        t_code = _TERMINAL_OTD_NAMES.get(t_full, t_full)
        date_raw = row.get("order_date", "")
        date_str = str(date_raw)[:10] if date_raw else ""
        loads_list.append({
            "date":         date_str,
            "order":        row.get("order_num", ""),
            "customer":     row.get("customer", ""),
            "carrier":      row.get("carrier", ""),
            "terminal":     t_code,
            "revenue":      rev,
            "carrier_cost": cost,
            "profit":       profit,
            "margin_pct":   margin,
        })

    return {
        "customers": customers,
        "loads":     loads_list,
        "all": {
            "loads":        total_loads,
            "revenue":      round(total_rev,  2),
            "carrier_cost": round(total_cost, 2),
            "profit":       total_profit,
            "margin_pct":   total_margin,
        },
    }


# ── Build full dashboard payload ──────────────────────────────────────────────

def _query_driver_availability() -> dict:
    """Query live driver availability by terminal from dwFacts_DriverAvailabilityLast.
    Uses Driver_Active=1 to get current SCD records only (no duplicates).
    Grouped by terminal so dispatch can see where available capacity sits.
    """
    _TERM_MAP = {
        "Houston":           "HOU",
        "Louisiana":         "LOU",
        "Dallas/Fort Worth": "DFW",
        "Oklahoma City":     "OKC",
    }
    sql = """
WITH avail AS (
    SELECT DISTINCT da.Driver_Key, d.Driver_Terminal
    FROM tmw_dwlive.dbo.dwFacts_DriverAvailabilityLast da
    JOIN tmw_dwlive.dbo.dwDriverDimension d ON da.Driver_Key = d.Driver_Key
    WHERE da.Driver_Key > 0
      AND d.Driver_Active = 1
      AND d.Driver_Terminal IN ('Houston','Louisiana','Dallas/Fort Worth','Oklahoma City')
),
total_drivers AS (
    SELECT Driver_Terminal, COUNT(*) AS driver_total
    FROM tmw_dwlive.dbo.dwDriverDimension
    WHERE Driver_Active = 1
      AND Driver_DateEnd = '2099-12-31'
      AND Driver_TerminationDate > GETDATE()
      AND Driver_Terminal IN ('Houston','Louisiana','Dallas/Fort Worth','Oklahoma City')
      AND Driver_Type1 != 'BrokerLoad'
      AND ISNULL(Driver_Firstname,'') NOT IN ('Unknown','NotDefined','BrokerLoad','')
      AND ISNULL(Driver_Teamleader,'') NOT IN ('NotDefined','')
    GROUP BY Driver_Terminal
)
SELECT
    t.Driver_Terminal                 AS terminal,
    COUNT(a.Driver_Key)               AS available,
    ISNULL(t.driver_total, 0)         AS driver_total
FROM total_drivers t
LEFT JOIN avail a ON a.Driver_Terminal = t.Driver_Terminal
GROUP BY t.Driver_Terminal, t.driver_total
"""
    try:
        rows   = _tmw_query(sql)
        result = {code: {"available": 0, "driver_total": 0} for code in ["HOU","LOU","DFW","OKC"]}
        for row in rows:
            code = _TERM_MAP.get(row.get("terminal",""), "")
            if code in result:
                result[code] = {
                    "available":     int(row.get("available")    or 0),
                    "driver_total":  int(row.get("driver_total") or 0),
                }
        all_avail  = sum(v["available"]    for v in result.values())
        all_total  = sum(v["driver_total"] for v in result.values())
        result["all"] = {"available": all_avail, "driver_total": all_total}
        return result
    except Exception as e:
        logging.warning(f"Driver availability query failed: {e}")
    return {"all": {"available": None, "driver_total": None}}


def _query_tractor_availability() -> dict:
    """Query live tractor availability count from TMW."""
    sql = """
SELECT COUNT(*) AS available
FROM tmw_dwlive.dbo.dwTractorDimension t
WHERE t.Tractor_Status = 'AVL'
  AND t.Tractor_Key > 0
"""
    try:
        rows = _tmw_query(sql)
        if rows:
            avail = int(rows[0].get("available") or 0)
            return {"available": avail}
    except Exception as e:
        logging.warning(f"Tractor availability query failed: {e}")
    return {"available": None}


def _query_cancelled_loads(since_date: str) -> dict:
    """Query cancelled (turned-down / rejected) EDI loads from TMW_Live.
    Customer identified via ord_billto: CHRBP = BP, SHENOV = Shell.
    EDI state 31 = JHW declined the tender.
    EDI state 36/37 = all treated as turn-downs.
    since_date: ISO date string (YYYY-MM-DD) — lower bound on ord_datetaken.
    """
    sql_summary = f"""
SELECT
    CASE
        WHEN o.ord_revtype2 = 'CR00'               THEN 'Oneton'
        WHEN o.ord_revtype2 = 'ID00'               THEN 'Minifloat'
        WHEN o.ord_revtype2 IN ('DD00','S/D','ZZ') THEN 'Tractor'
        ELSE COALESCE(lv2.name, o.ord_revtype2, 'Unknown')
    END                                             AS equip_type,
    o.ord_revtype2                                  AS equip_code,
    COALESCE(lv4.name, o.ord_revtype4, 'Unknown') AS trailer_type,
    o.ord_revtype4                                  AS trailer_code,
    CASE WHEN o.ord_billto = 'CHRBP'  THEN 'BP'
         WHEN o.ord_billto = 'SHENOV' THEN 'Shell'
         ELSE COALESCE(bc.cmp_name, o.ord_billto, 'Other') END AS customer,
    COUNT(*)                                        AS load_count,
    COUNT(*)                                        AS turned_down
FROM TMW_Live.dbo.orderheader o
LEFT JOIN TMW_Live.dbo.company bc ON bc.cmp_id = o.ord_billto
LEFT JOIN TMW_Live.dbo.labelfile lv2
    ON lv2.labeldefinition = 'RevType2' AND lv2.abbr = o.ord_revtype2
LEFT JOIN TMW_Live.dbo.labelfile lv4
    ON lv4.labeldefinition = 'RevType4' AND lv4.abbr = o.ord_revtype4
WHERE o.ord_status = 'CAN'
  AND o.ord_datetaken >= '{since_date}'
  AND o.ord_editradingpartner IS NOT NULL
GROUP BY
    CASE
        WHEN o.ord_revtype2 = 'CR00'               THEN 'Oneton'
        WHEN o.ord_revtype2 = 'ID00'               THEN 'Minifloat'
        WHEN o.ord_revtype2 IN ('DD00','S/D','ZZ') THEN 'Tractor'
        ELSE COALESCE(lv2.name, o.ord_revtype2, 'Unknown')
    END,
    o.ord_revtype2,
    COALESCE(lv4.name, o.ord_revtype4, 'Unknown'),
    o.ord_revtype4,
    CASE WHEN o.ord_billto = 'CHRBP'  THEN 'BP'
         WHEN o.ord_billto = 'SHENOV' THEN 'Shell'
         ELSE COALESCE(bc.cmp_name, o.ord_billto, 'Other') END
ORDER BY load_count DESC
"""
    sql_detail = f"""
SELECT TOP 500
    RTRIM(o.ord_number)                             AS order_num,
    CASE WHEN o.ord_billto = 'CHRBP'  THEN 'BP'
         WHEN o.ord_billto = 'SHENOV' THEN 'Shell'
         ELSE COALESCE(bc.cmp_name, o.ord_billto, 'Other') END AS customer,
    COALESCE(shp.cmp_name, o.ord_shipper, '')       AS shipper,
    COALESCE(con.cmp_name, o.ord_consignee, '')      AS consignee,
    CONVERT(varchar(16), o.ord_startdate, 120)      AS pickup_dt,
    CONVERT(varchar(16), o.ord_completiondate, 120) AS delivery_dt,
    CONVERT(varchar(16), o.ord_datetaken, 120)      AS tendered_dt,
    CASE
        WHEN o.ord_revtype2 = 'CR00'               THEN 'Oneton'
        WHEN o.ord_revtype2 = 'ID00'               THEN 'Minifloat'
        WHEN o.ord_revtype2 IN ('DD00','S/D','ZZ') THEN 'Tractor'
        ELSE COALESCE(lv2.name, o.ord_revtype2, 'Unknown')
    END                                              AS equip_type,
    COALESCE(lv4.name, o.ord_revtype4, 'Unknown')   AS trailer_type,
    COALESCE(o.ord_revtype1, 'UNK')                 AS terminal,
    CASE WHEN o.ord_edistate = 31
              THEN COALESCE(NULLIF(RTRIM(o.ord_edideclinereason),''), 'Declined')
         WHEN o.ord_edistate IN (36,37) THEN 'Declined'
         WHEN o.ord_edistate = 38       THEN 'Cancel Declined'
         ELSE 'Declined' END                         AS reason
FROM TMW_Live.dbo.orderheader o
LEFT JOIN TMW_Live.dbo.company bc  ON bc.cmp_id  = o.ord_billto
LEFT JOIN TMW_Live.dbo.company shp ON shp.cmp_id = o.ord_shipper
LEFT JOIN TMW_Live.dbo.company con ON con.cmp_id = o.ord_consignee
LEFT JOIN TMW_Live.dbo.labelfile lv2
    ON lv2.labeldefinition = 'RevType2' AND lv2.abbr = o.ord_revtype2
LEFT JOIN TMW_Live.dbo.labelfile lv4
    ON lv4.labeldefinition = 'RevType4' AND lv4.abbr = o.ord_revtype4
WHERE o.ord_status = 'CAN'
  AND o.ord_datetaken >= '{since_date}'
  AND o.ord_editradingpartner IS NOT NULL
ORDER BY o.ord_datetaken DESC
"""
    summary_rows = _tmw_query(sql_summary)
    detail_rows  = _tmw_query(sql_detail)

    summary = []
    for r in summary_rows:
        summary.append({
            "equip_type":   r.get("equip_type", "Unknown"),
            "equip_code":   r.get("equip_code", ""),
            "trailer_type": r.get("trailer_type", "Unknown"),
            "trailer_code": r.get("trailer_code", ""),
            "customer":     r.get("customer", "Other"),
            "load_count":   int(r.get("load_count") or 0),
            "turned_down":  int(r.get("turned_down") or 0),
        })

    detail = []
    for r in detail_rows:
        detail.append({
            "order_num":   r.get("order_num", ""),
            "customer":    r.get("customer", ""),
            "shipper":     r.get("shipper", ""),
            "consignee":   r.get("consignee", ""),
            "pickup_dt":   r.get("pickup_dt", ""),
            "delivery_dt": r.get("delivery_dt", ""),
            "tendered_dt": r.get("tendered_dt", ""),
            "equip_type":  r.get("equip_type", ""),
            "trailer_type":r.get("trailer_type", ""),
            "terminal":    r.get("terminal", ""),
            "reason":      r.get("reason", ""),
        })

    total       = sum(r["load_count"] for r in summary)
    shell_total = sum(r["load_count"] for r in summary if r["customer"] == "Shell")
    bp_total    = sum(r["load_count"] for r in summary if r["customer"] == "BP")
    turned_down = sum(r["turned_down"] for r in summary)

    return {
        "summary": summary,
        "detail":  detail,
        "totals": {
            "total":       total,
            "shell":       shell_total,
            "bp":          bp_total,
            "turned_down": turned_down,
        },
    }


def _get_graph_token() -> str:
    """Obtain a Microsoft Graph app-only access token via client credentials."""
    tenant_id     = os.environ.get("GRAPH_TENANT_ID", "")
    client_id     = os.environ.get("GRAPH_CLIENT_ID", "")
    client_secret = os.environ.get("GRAPH_CLIENT_SECRET", "")
    if not (tenant_id and client_id and client_secret):
        raise RuntimeError("GRAPH_TENANT_ID / GRAPH_CLIENT_ID / GRAPH_CLIENT_SECRET not configured")
    post_data = urllib.parse.urlencode({
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "scope":         "https://graph.microsoft.com/.default",
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data=post_data,
    )
    with urllib.request.urlopen(req, timeout=15) as _r:
        return json.loads(_r.read().decode("utf-8"))["access_token"]


def _query_oxy_excel_loads(since_date: str) -> list:
    """
    Read OXY Load Tracking.xlsx from SharePoint and return turned-down rows
    (Accepted == 'No') with tender date >= since_date and <= today.
    Rows are shaped to match the detail structure of _query_cancelled_loads.
    Returns [] on any error so the dashboard still renders with TMW data.
    """
    try:
        token   = _get_graph_token()
        g_hdrs  = {"Authorization": f"Bearer {token}"}
        base    = (f"https://graph.microsoft.com/v1.0"
                   f"/drives/{_OXY_SP_DRIVE_ID}/items/{_OXY_SP_ITEM_ID}/workbook")

        # Known column indices (confirmed from header inspection of the live sheet):
        _COL_JOB_ID    = 0   # JobID
        _COL_TENDER_DT = 1   # TenderDateTime (ISO-8601 with tz, e.g. 2026-03-31T14:02:57+00:00)
        _COL_EQUIPMENT = 7   # Equipment
        _COL_ORIGIN    = 14  # Origin
        _COL_DEST      = 21  # Destination
        _COL_ACCEPTED  = 33  # Accepted ("Yes" / "No")
        # Sheet has 36 columns (A-AJ). usedRange is too slow on this large workbook
        # (returns 504 Gateway Timeout), so we read a fixed tail window instead.
        # Window covers ~1.7 months at ~100 rows/day; empty rows are filtered out.
        _START_ROW  = 7500   # read from row 7500 so we cover several months of history
        _END_ROW    = 12000  # generous upper bound; Graph returns up to last real row
        range_addr  = f"A{_START_ROW}:AJ{_END_ROW}"

        # Read only the tail slice of the sheet (well under the 5 MB payload limit)
        logging.info(f"OXY Excel: reading fixed range {range_addr}")
        _rng_req = urllib.request.Request(
            f"{base}/worksheets('Sheet1')/range(address='{range_addr}')",
            headers=g_hdrs,
        )
        with urllib.request.urlopen(_rng_req, timeout=90) as _r:
            values = json.loads(_r.read().decode("utf-8")).get("values", [])
        if not values:
            return []

        since_dt = datetime.datetime.strptime(since_date[:10], "%Y-%m-%d").date()
        today    = _today_central()

        result = []
        for row in values:
            if len(row) <= _COL_ACCEPTED:
                continue
            if str(row[_COL_ACCEPTED]).strip().lower() != "no":
                continue

            raw_date = str(row[_COL_TENDER_DT]).strip() if len(row) > _COL_TENDER_DT else ""
            row_date = None
            if raw_date:
                # ISO format: "2026-03-31T14:02:57+00:00" or "2026-03-31 ..." or "2026-03-31"
                # Take the first 10 chars (YYYY-MM-DD) to avoid timezone/time suffix issues
                if len(raw_date) >= 10 and raw_date[4:5] == '-' and raw_date[7:8] == '-':
                    try:
                        row_date = datetime.datetime.strptime(raw_date[:10], "%Y-%m-%d").date()
                    except (ValueError, TypeError):
                        pass
                # US date formats fallback: "03/31/2026" or "3/31/26"
                if row_date is None:
                    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
                        try:
                            row_date = datetime.datetime.strptime(raw_date.split()[0], fmt).date()
                            break
                        except (ValueError, TypeError):
                            continue

            if row_date is None or row_date < since_dt or row_date > today:
                continue

            equip_raw  = str(row[_COL_EQUIPMENT]).strip() if len(row) > _COL_EQUIPMENT else ""
            equip_type = _OXY_EQUIP_MAP.get(equip_raw.lower(), equip_raw or "Unknown")
            job_id     = str(row[_COL_JOB_ID]).strip()  if len(row) > _COL_JOB_ID  else ""
            origin     = str(row[_COL_ORIGIN]).strip()  if len(row) > _COL_ORIGIN  else ""
            dest       = str(row[_COL_DEST]).strip()    if len(row) > _COL_DEST    else ""

            result.append({
                "order_num":    job_id,
                "customer":     "OXY",
                "shipper":      origin,
                "consignee":    dest,
                "pickup_dt":    "",
                "delivery_dt":  "",
                "tendered_dt":  raw_date[:10],
                "equip_type":   equip_type,
                "trailer_type": "",
                "terminal":     "",
                "reason":       "Turned Down",
                "source":       "excel",
            })

        logging.info(f"OXY Excel: {len(result)} turned-down loads since {since_date}")
        return result

    except Exception as e:
        logging.warning(f"OXY Excel load query failed: {e}")
        return []


def _merge_oxy_into_cancelled(cancelled: dict, excel_rows: list) -> dict:
    """Merge OXY Excel turned-down rows into a _query_cancelled_loads result dict."""
    if not excel_rows:
        return cancelled

    # Merge detail list
    cancelled["detail"].extend(excel_rows)
    cancelled["detail"].sort(key=lambda x: x.get("tendered_dt") or "", reverse=True)

    # Update summary — group Excel rows by equip_type under customer "OXY"
    from collections import defaultdict
    excel_by_equip: dict = defaultdict(int)
    for row in excel_rows:
        excel_by_equip[row["equip_type"]] += 1

    for equip, count in excel_by_equip.items():
        existing = next(
            (s for s in cancelled["summary"]
             if s["customer"] == "OXY" and s["equip_type"] == equip),
            None,
        )
        if existing:
            existing["load_count"]  += count
            existing["turned_down"] += count
        else:
            cancelled["summary"].append({
                "equip_type":   equip,
                "equip_code":   "",
                "trailer_type": "",
                "trailer_code": "",
                "customer":     "OXY",
                "load_count":   count,
                "turned_down":  count,
            })

    # Update totals
    n = len(excel_rows)
    cancelled["totals"]["total"]       += n
    cancelled["totals"]["turned_down"] += n
    cancelled["totals"]["oxy"]          = cancelled["totals"].get("oxy", 0) + n
    return cancelled


def build_dashboard_data() -> dict:
    payload = {"generatedAt": datetime.datetime.now(_CENTRAL).isoformat(timespec="minutes")}

    # Live availability (not period-dependent)
    logging.info("Querying live driver & tractor availability...")
    payload["driverAvailability"]  = _query_driver_availability()
    payload["tractorAvailability"] = _query_tractor_availability()

    for period in ("today", "week", "month"):
        logging.info(f"Querying TMW for period: {period}")
        start = _period_start(period)
        try:
            terminals      = _query_terminals(start)
            customers      = _query_customers(start)
            term_custs     = _query_terminal_customers(start)
            drivers        = _query_drivers(start)
            driver_loads   = _query_driver_loads(start)
            customer_loads = _query_customer_loads(start)
            otd            = _query_otd(start)
            brokered_loads = _query_brokered_loads(start)
        except Exception as e:
            logging.error(f"Query failed for {period}: {e}")
            continue
        total_rev    = sum(t["revenue"]      for t in terminals.values())
        total_loads  = sum(t["loads"]        for t in terminals.values())
        total_loaded = sum(t["loaded_miles"] for t in terminals.values())
        total_empty  = sum(t["empty_miles"]  for t in terminals.values())
        total_miles  = total_loaded + total_empty
        payload[period] = {
            "label":             _period_label(period),
            "terminals":         terminals,
            "totalRevenue":      round(total_rev, 2),
            "totalLoads":        total_loads,
            "revPerMile":        round(total_rev / total_miles, 2) if total_miles else 0,
            "loadedMilePct":     round(total_loaded / total_miles * 100, 1) if total_miles else 0,
            "topCustomers":      customers,
            "terminalCustomers": term_custs,
            "topDrivers":        drivers,
            "driverLoads":       driver_loads,
            "customerLoads":     customer_loads,
            "brokeredLoads":     brokered_loads,
            "otd":               otd,
        }
    logging.info("Querying TMW for trend data (24-month history)...")
    try:
        payload["trends"] = _query_trends()
    except Exception as e:
        logging.error(f"Trend query failed: {e}")
        payload["trends"] = []
    logging.info("Querying cancelled/turned-down loads (today / week / month)...")
    empty_cancel = {"summary": [], "detail": [], "totals": {}}
    cancel_by_period = {}
    for p in ("today", "week", "month"):
        try:
            tmw_data   = _query_cancelled_loads(_period_start(p))
            excel_rows = _query_oxy_excel_loads(_period_start(p))
            cancel_by_period[p] = _merge_oxy_into_cancelled(tmw_data, excel_rows)
        except Exception as e:
            logging.error(f"Cancelled loads query failed for period={p}: {e}")
            cancel_by_period[p] = empty_cancel
    payload["cancelledLoads"] = cancel_by_period
    return payload


# ── Local file output (replaces Azure Blob upload) ────────────────────────────

def write_to_disk(data: dict) -> None:
    os.makedirs(DASHBOARD_OUTPUT_DIR, exist_ok=True)

    # Write the raw JSON data file (fetched dynamically by the HTML)
    json_path = os.path.join(DASHBOARD_OUTPUT_DIR, "dashboard-data.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    logging.info(f"Data written -> {json_path}")

    # Also build the self-contained HTML (data injected inline) for offline use
    if os.path.exists(DASHBOARD_TEMPLATE):
        with open(DASHBOARD_TEMPLATE, "r", encoding="utf-8") as f:
            html_text = f.read()

        # Remove any previously injected data block
        html_text = re.sub(
            r"<script>window\.__DASHBOARD_DATA__\s*=.*?;</script>\s*\n?",
            "",
            html_text,
            flags=re.DOTALL,
        )

        # Inject fresh data
        json_str  = json.dumps(data, ensure_ascii=False)
        inject    = f"<script>window.__DASHBOARD_DATA__ = {json_str};</script>\n"
        html_text = html_text.replace("</head>", inject + "</head>", 1)

        html_path = os.path.join(DASHBOARD_OUTPUT_DIR, "operations-dashboard.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_text)
        logging.info(f"HTML written  -> {html_path}")
    else:
        logging.warning(f"Template not found at {DASHBOARD_TEMPLATE} — skipping HTML output")


# ── Entry point — called by cron every 15 minutes ─────────────────────────────

def refresh_dashboard() -> None:
    logging.info("JHW Dashboard refresh starting...")
    data = build_dashboard_data()
    total = sum(t["revenue"] for t in data.get("today", {}).get("terminals", {}).values())
    logging.info(f"Data ready — Today revenue: ${total:,.0f}")
    write_to_disk(data)
    logging.info("Refresh complete.")


if __name__ == "__main__":
    try:
        refresh_dashboard()
    except Exception as e:
        logging.error(f"Dashboard refresh failed: {e}", exc_info=True)
        raise SystemExit(1)
