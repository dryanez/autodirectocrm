"""
db.py — Supabase-backed database adapter for SimplyAPI.

Replaces sqlite3 with Supabase REST API calls, using the same
.execute() / .fetchone() / .fetchall() / .commit() interface
so app.py needs minimal changes.

All CRM tables live in Supabase Postgres instead of a local SQLite file.
"""

import os
import json
import re
import requests as _req
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_ANON_KEY", "")
)


def _headers(prefer_return=True):
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer_return:
        h["Prefer"] = "return=representation"
    return h


def _rest(table):
    return f"{SUPABASE_URL}/rest/v1/{table}"


# ─── SQL Parser ───────────────────────────────────────────────────────────────
# Translates the SQLite-style SQL used in app.py into Supabase REST API calls.
# Supports the subset of SQL actually used: SELECT, INSERT, UPDATE, DELETE,
# CREATE TABLE IF NOT EXISTS (ignored — tables created via Supabase dashboard),
# ALTER TABLE (ignored), CREATE INDEX (ignored).

class SupabaseResult:
    """Mimics sqlite3 cursor result — list of dict-like rows."""
    def __init__(self, rows=None, last_id=None):
        self._rows = rows or []
        self._last_id = last_id

    def fetchall(self):
        return self._rows

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows[0]

    def __iter__(self):
        return iter(self._rows)

    def __len__(self):
        return len(self._rows)


class SupabaseRow(dict):
    """Dict subclass that also supports attribute access (row["col"] and row.col)."""
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def keys(self):
        return super().keys()


def _make_rows(data):
    if data is None:
        return []
    if isinstance(data, list):
        return [SupabaseRow(r) for r in data]
    if isinstance(data, dict):
        return [SupabaseRow(data)]
    return []


class SupabaseConn:
    """
    Drop-in replacement for sqlite3 connection.
    Translates SQL to Supabase REST calls.
    """

    def __init__(self):
        self._last_insert_id = None

    def execute(self, sql, params=None):
        sql = sql.strip()
        params = list(params) if params else []

        # Skip DDL — tables are created via Supabase dashboard
        upper = sql.upper()
        if (upper.startswith("CREATE TABLE") or
                upper.startswith("CREATE INDEX") or
                upper.startswith("ALTER TABLE") or
                upper.startswith("DROP TABLE") or
                upper.startswith("PRAGMA")):
            return SupabaseResult()

        # SELECT last_insert_rowid() → return cached last id
        if "LAST_INSERT_ROWID" in upper:
            return SupabaseResult([SupabaseRow({"last_insert_rowid()": self._last_insert_id})])

        # SELECT COUNT(*)
        if upper.startswith("SELECT COUNT(*)"):
            table, filters = _parse_where(sql, params)
            rows = _supa_get(table, filters, select="id")
            count = len(rows)
            return SupabaseResult([SupabaseRow({"COUNT(*)": count})])

        # SELECT
        if upper.startswith("SELECT"):
            table, select_cols, filters, order, limit = _parse_select(sql, params)
            rows = _supa_get(table, filters, select=select_cols, order=order, limit=limit)
            return SupabaseResult(_make_rows(rows))

        # INSERT
        if upper.startswith("INSERT INTO"):
            table, record = _parse_insert(sql, params)
            result = _supa_insert(table, record)
            if result:
                self._last_insert_id = result[0].get("id")
                return SupabaseResult(_make_rows(result))
            return SupabaseResult()

        # UPDATE
        if upper.startswith("UPDATE"):
            table, updates, filters = _parse_update(sql, params)
            _supa_update(table, updates, filters)
            return SupabaseResult()

        # DELETE
        if upper.startswith("DELETE FROM"):
            table, filters = _parse_where(sql, params)
            _supa_delete(table, filters)
            return SupabaseResult()

        return SupabaseResult()

    def commit(self):
        pass  # Auto-committed in Supabase REST

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


# ─── REST Helpers ─────────────────────────────────────────────────────────────

def _supa_get(table, filters=None, select="*", order=None, limit=None):
    params = {"select": select or "*"}
    if filters:
        for k, v in filters.items():
            params[k] = v
    if order:
        params["order"] = order
    if limit:
        params["limit"] = limit
    try:
        r = _req.get(_rest(table), params=params, headers=_headers(prefer_return=False), timeout=10)
        if r.status_code == 200:
            return r.json()
        print(f"[db] GET {table} {r.status_code}: {r.text[:200]}")
        return []
    except Exception as e:
        print(f"[db] GET {table} error: {e}")
        return []


def _supa_insert(table, record):
    try:
        r = _req.post(_rest(table), json=record, headers=_headers(), timeout=10)
        if r.status_code in (200, 201):
            return r.json() if isinstance(r.json(), list) else [r.json()]
        print(f"[db] INSERT {table} {r.status_code}: {r.text[:300]}")
        return []
    except Exception as e:
        print(f"[db] INSERT {table} error: {e}")
        return []


def _supa_update(table, updates, filters):
    params = {}
    for k, v in (filters or {}).items():
        params[k] = v
    try:
        r = _req.patch(_rest(table), json=updates, params=params, headers=_headers(), timeout=10)
        if r.status_code not in (200, 204):
            print(f"[db] UPDATE {table} {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[db] UPDATE {table} error: {e}")


def _supa_delete(table, filters):
    params = {}
    for k, v in (filters or {}).items():
        params[k] = v
    try:
        r = _req.delete(_rest(table), params=params, headers=_headers(prefer_return=False), timeout=10)
        if r.status_code not in (200, 204):
            print(f"[db] DELETE {table} {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[db] DELETE {table} error: {e}")


# ─── SQL Parsers ──────────────────────────────────────────────────────────────
# These parse the specific SQL patterns used in app.py.

def _extract_table(sql):
    """Extract table name from SQL."""
    m = re.search(r'(?:FROM|INTO|UPDATE|TABLE)\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.I)
    return m.group(1).lower() if m else None


def _bind_params(sql, params):
    """Replace ? placeholders with actual values (for parsing)."""
    result = []
    param_iter = iter(params)
    for char in sql:
        if char == '?':
            try:
                result.append(repr(next(param_iter)))
            except StopIteration:
                result.append('NULL')
        else:
            result.append(char)
    return ''.join(result)


def _parse_where_clause(where_str, params):
    """
    Parse a WHERE clause into Supabase filter params.
    Handles: col=?, col=value, col LIKE %, col IS NULL,
    UPPER(col)=UPPER(?), col IN (...), col BETWEEN ? AND ?,
    col>=?, col<=?, col>?, col<?
    """
    filters = {}
    if not where_str:
        return filters, params

    where_str = where_str.strip()
    param_list = list(params)
    param_idx = [0]

    def next_param():
        if param_idx[0] < len(param_list):
            v = param_list[param_idx[0]]
            param_idx[0] += 1
            return v
        return None

    # Split on AND (simple, doesn't handle OR or nested parens)
    conditions = re.split(r'\bAND\b', where_str, flags=re.I)
    for cond in conditions:
        cond = cond.strip()
        if not cond or cond == '1=1':
            continue

        # UPPER(col)=UPPER(?)
        m = re.match(r"UPPER\(([^)]+)\)\s*=\s*UPPER\(\?\)", cond, re.I)
        if m:
            col = m.group(1).strip()
            v = next_param()
            filters[col] = f"ilike.{v}"
            continue

        # col BETWEEN ? AND ?
        m = re.match(r"([a-zA-Z_.]+)\s+BETWEEN\s+\?\s+AND\s+\?", cond, re.I)
        if m:
            col = m.group(1).strip()
            v1 = next_param()
            v2 = next_param()
            filters[col] = f"gte.{v1}"
            filters[f"{col}_lte"] = f"lte.{v2}"  # Note: Supabase handles this with separate params
            continue

        # col >= ? / col <= ? / col > ? / col < ?
        m = re.match(r"([a-zA-Z_.]+)\s*(>=|<=|>|<)\s*\?", cond, re.I)
        if m:
            col, op = m.group(1).strip(), m.group(2)
            v = next_param()
            op_map = {">=": "gte", "<=": "lte", ">": "gt", "<": "lt"}
            filters[col] = f"{op_map[op]}.{v}"
            continue

        # col LIKE ?
        m = re.match(r"([a-zA-Z_.]+)\s+LIKE\s+\?", cond, re.I)
        if m:
            col = m.group(1).strip()
            v = next_param()
            # Convert SQL LIKE % to Supabase ilike
            filters[col] = f"ilike.{v}"
            continue

        # col IS NULL
        m = re.match(r"([a-zA-Z_.]+)\s+IS\s+NULL", cond, re.I)
        if m:
            col = m.group(1).strip()
            filters[col] = "is.null"
            continue

        # col IS NOT NULL
        m = re.match(r"([a-zA-Z_.]+)\s+IS\s+NOT\s+NULL", cond, re.I)
        if m:
            col = m.group(1).strip()
            filters[col] = "not.is.null"
            continue

        # col IN (...)
        m = re.match(r"([a-zA-Z_.]+)\s+IN\s*\(([^)]+)\)", cond, re.I)
        if m:
            col = m.group(1).strip()
            vals = [v.strip().strip("'\"") for v in m.group(2).split(",")]
            filters[col] = f"in.({','.join(vals)})"
            continue

        # col NOT IN (...)
        m = re.match(r"([a-zA-Z_.]+)\s+NOT\s+IN\s*\(([^)]+)\)", cond, re.I)
        if m:
            col = m.group(1).strip()
            vals = [v.strip().strip("'\"") for v in m.group(2).split(",")]
            filters[col] = f"not.in.({','.join(vals)})"
            continue

        # col=? or col='value' or col=value
        m = re.match(r"([a-zA-Z_.]+)\s*=\s*\?", cond, re.I)
        if m:
            col = m.group(1).strip()
            v = next_param()
            filters[col] = f"eq.{v}"
            continue

        m = re.match(r"([a-zA-Z_.]+)\s*=\s*'([^']*)'", cond, re.I)
        if m:
            col, v = m.group(1).strip(), m.group(2)
            filters[col] = f"eq.{v}"
            continue

        m = re.match(r"([a-zA-Z_.]+)\s*=\s*(\w+)", cond, re.I)
        if m:
            col, v = m.group(1).strip(), m.group(2)
            if v.upper() != 'NULL':
                filters[col] = f"eq.{v}"
            continue

    remaining_params = param_list[param_idx[0]:]
    return filters, remaining_params


def _parse_select(sql, params):
    """Parse SELECT ... FROM table [WHERE ...] [ORDER BY ...] [LIMIT n]"""
    table = _extract_table(sql)

    # Extract SELECT columns
    m = re.match(r"SELECT\s+(.*?)\s+FROM\s+", sql, re.I | re.S)
    select_cols = "*"
    if m:
        cols = m.group(1).strip()
        # Handle table aliases like "c.*, u.name as assigned_user_name"
        # For Supabase we just request * and filter client-side if needed
        if cols != "*" and ".*" not in cols and "COUNT" not in cols.upper():
            # Clean up aliases
            clean_cols = []
            for col in cols.split(","):
                col = col.strip()
                # Remove table prefix (c.id → id)
                col = re.sub(r'^[a-z]\.\*$', '*', col)
                col = re.sub(r'^[a-z]\.', '', col)
                # Remove AS alias for now (Supabase doesn't need it)
                col = re.sub(r'\s+AS\s+\w+', '', col, flags=re.I)
                if col and col != '*':
                    clean_cols.append(col)
            if clean_cols and '*' not in cols:
                select_cols = ",".join(clean_cols)

    # Extract WHERE clause
    where_match = re.search(r'\bWHERE\b(.*?)(?:\bORDER BY\b|\bLIMIT\b|$)', sql, re.I | re.S)
    filters = {}
    remaining_params = params
    if where_match:
        filters, remaining_params = _parse_where_clause(where_match.group(1), params)

    # Extract ORDER BY
    order = None
    order_match = re.search(r'ORDER BY\s+(.*?)(?:\bLIMIT\b|$)', sql, re.I | re.S)
    if order_match:
        order_str = order_match.group(1).strip()
        # Convert "col DESC" to "col.desc", "col ASC" to "col.asc"
        order_parts = []
        for part in order_str.split(","):
            part = part.strip()
            if re.search(r'\bDESC\b', part, re.I):
                col = re.sub(r'\s+DESC\b.*', '', part, flags=re.I).strip()
                col = re.sub(r'^[a-z]\.', '', col)
                order_parts.append(f"{col}.desc")
            else:
                col = re.sub(r'\s+ASC\b.*', '', part, flags=re.I).strip()
                col = re.sub(r'^[a-z]\.', '', col)
                order_parts.append(f"{col}.asc")
        order = ",".join(order_parts)

    # Extract LIMIT
    limit = None
    limit_match = re.search(r'LIMIT\s+(\d+)', sql, re.I)
    if limit_match:
        limit = int(limit_match.group(1))

    return table, select_cols, filters, order, limit


def _parse_insert(sql, params):
    """Parse INSERT INTO table (cols) VALUES (vals)"""
    table = _extract_table(sql)

    # Extract column names
    cols_match = re.search(r'INSERT INTO \w+\s*\(([^)]+)\)', sql, re.I)
    if not cols_match:
        return table, {}

    cols = [c.strip() for c in cols_match.group(1).split(",")]
    record = {}
    for i, col in enumerate(cols):
        if i < len(params):
            record[col] = params[i]

    return table, record


def _parse_update(sql, params):
    """Parse UPDATE table SET col=?, ... WHERE col=?"""
    table = _extract_table(sql)

    # Extract SET clause
    set_match = re.search(r'SET\s+(.*?)\s+WHERE\s+', sql, re.I | re.S)
    if not set_match:
        # UPDATE without WHERE
        set_match = re.search(r'SET\s+(.*?)$', sql, re.I | re.S)

    updates = {}
    set_params_used = 0
    if set_match:
        set_str = set_match.group(1)
        set_parts = re.split(r',\s*(?=[a-zA-Z_])', set_str)
        for part in set_parts:
            m = re.match(r'([a-zA-Z_]+)\s*=\s*\?', part.strip())
            if m:
                col = m.group(1)
                if set_params_used < len(params):
                    updates[col] = params[set_params_used]
                    set_params_used += 1

    # Extract WHERE clause
    where_match = re.search(r'WHERE\s+(.*?)$', sql, re.I | re.S)
    filters = {}
    if where_match:
        filters, _ = _parse_where_clause(where_match.group(1), params[set_params_used:])

    return table, updates, filters


def _parse_where(sql, params):
    """Parse DELETE/COUNT WHERE clause."""
    table = _extract_table(sql)
    where_match = re.search(r'WHERE\s+(.*?)$', sql, re.I | re.S)
    filters = {}
    if where_match:
        filters, _ = _parse_where_clause(where_match.group(1), params)
    return table, filters


# ─── Public API ───────────────────────────────────────────────────────────────

def get_conn():
    """Returns a Supabase-backed connection (drop-in for sqlite3.connect)."""
    return SupabaseConn()


def row_to_dict(row):
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    # sqlite3.Row fallback
    try:
        return dict(row)
    except Exception:
        return {}


def init_db():
    """
    Ensure all CRM tables exist in Supabase.
    Run this SQL once in the Supabase SQL editor:
    https://supabase.com/dashboard/project/kqympdxeszdyppbhtzbm/sql/new
    """
    pass  # Tables are created via Supabase dashboard SQL (see setup_crm.sql)


def get_db():
    return get_conn()


def get_crm_conn():
    return get_conn()
