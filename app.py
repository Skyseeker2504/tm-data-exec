
# app.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import duckdb, sqlite3, pandas as pd, os, json, pathlib, datetime as dt
app = FastAPI(title="data-exec")
# ========= ENV / PATHS =========
DATA_ROOT   = os.getenv("DATA_ROOT", r"E:\data_growth_agent")  # base folder for JSONs etc.
JSON_DIR    = os.getenv("JSON_DIR",  DATA_ROOT)
ASSETS_DIR  = os.getenv("ASSETS_DIR", os.path.join(DATA_ROOT, "assets"))
# DuckDB / SQLite data files
DUCK_SERVICES   = os.getenv("DUCK_SERVICES",   r"D:\tmdata_services\contracts_db\services_contracts.duckdb")
DUCK_MINISTRY   = os.getenv("DUCK_MINISTRY",   r"D:\tmdata_ministry\contracts_db\ministry_contracts.duckdb")
DUCK_STATE      = os.getenv("DUCK_STATE",      r"D:\tmdata_state\contracts_db\state_contracts.duckdb")
SQLITE_PRODUCTS = os.getenv("SQLITE_PRODUCTS", r"D:\tmdata\tm_dedup.db")
# Table names produced by your loaders
DUCK_SERVICES_TABLE = os.getenv("DUCK_SERVICES_TABLE", "contracts_raw")
DUCK_MINISTRY_TABLE = os.getenv("DUCK_MINISTRY_TABLE", "contracts_raw")
DUCK_STATE_TABLE    = os.getenv("DUCK_STATE_TABLE",    "contracts_raw")
# Excel passthrough
EXCEL_PATH = os.getenv("EXCEL_PATH", os.path.join(DATA_ROOT, "docs", "gem_categories.xlsx"))
# Optional BM25 indexes
BM25_DB = {
    "text":  os.path.join(DATA_ROOT, "indices", "text_bm25.sqlite"),
    "ppt":   os.path.join(DATA_ROOT, "indices", "ppt_bm25.sqlite"),
    "media": os.path.join(DATA_ROOT, "indices", "media_bm25.sqlite"),
}
# ========= MODELS =========
class SqlIn(BaseModel):       sql: str
class SqlNamedIn(BaseModel):  db: str; sql: str
class ExcelIn(BaseModel):     sheet: str
class QIn(BaseModel):         q: str; k: int = 8; index: str = "text"
class JsonGetIn(BaseModel):   name: str; key: str | None = None
# ========= HELPERS =========
def _norm_path(p: str) -> str:
    return pathlib.Path(p).as_posix()
# ---- add these tiny helpers near the other helpers ----
def _get_list(obj, key: str):
    """Return obj[key] if it's a list; else an empty list. If obj itself is a list, return it."""
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        v = obj.get(key, [])
        return v if isinstance(v, list) else []
    return []
def _safe_register_json_rows(rows, view_name: str, fields: list[str] | None = None):
    """Register rows (list[dict]) as a view; if not a list, register empty."""
    if not isinstance(rows, list):
        rows = []
    df = pd.DataFrame(rows)
    # ensure missing columns exist so DESCRIBE/SAMPLE don’t break later
    if fields:
        for f in fields:
            if f not in df.columns:
                df[f] = pd.NA
        df = df[fields]
    _register_df_as_view(df, view_name)

def _detect_sqlite_table(db_path: str) -> str:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    t = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name LIMIT 1"
    ).fetchone()
    con.close()
    if not t:
        raise RuntimeError(f"No tables found in {db_path}")
    return t[0]
def _sqlite_has_table(db_path: str, table: str) -> bool:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (table,)
    ).fetchone()
    con.close()
    return bool(row)
def _sqlite_columns(db_path: str, table: str) -> set[str]:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cols = {r[1] for r in cur.execute(f"PRAGMA table_info({table});").fetchall()}
    con.close()
    return cols
def _pick_products_table(db_path: str) -> str:
    # Prefer the standardized table created by your converter
    if _sqlite_has_table(db_path, "tenders"):
        return "tenders"
    # Fallback to first table if tenders isn't present
    return _detect_sqlite_table(db_path)
def _duck_attach_readonly(conn: duckdb.DuckDBPyConnection, db_path: str, alias: str):
    conn.execute(f"ATTACH '{_norm_path(db_path)}' AS {alias} (READ_ONLY);")
def _json_path(name: str) -> str:
    return _norm_path(os.path.join(JSON_DIR, name))
def _register_df_as_view(df: pd.DataFrame, view: str):
    # Best-effort unregister then register
    try:
        DUCK.unregister(view)
    except Exception:
        pass
    DUCK.register(view, df)
    DUCK.execute(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {view}")
# ========= IN-MEMORY DUCKDB =========
DUCK = duckdb.connect(database=":memory:")
DUCK.execute("PRAGMA threads=4;")
DUCK.execute("INSTALL sqlite; LOAD sqlite;")
# ========= SQL SOURCES (Products SQLite + 3 DuckDBs) =========
def mount_sql_sources():
    # Serve static assets (optional)
    if ASSETS_DIR and os.path.isdir(ASSETS_DIR):
        app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")
    # =================== PRODUCTS (SQLite) — raw + normalized ===================
    prod_table = os.getenv("SQLITE_PRODUCTS_TABLE") or _pick_products_table(SQLITE_PRODUCTS)
    DUCK.execute(f"""
      CREATE OR REPLACE VIEW v_products_raw AS
      SELECT * FROM sqlite_scan('{_norm_path(SQLITE_PRODUCTS)}', '{prod_table}');
    """)
    # Introspect the SQLite table to decide whether to use t.department or t.dept
    prod_cols = _sqlite_columns(SQLITE_PRODUCTS, prod_table)
    department_expr = (
        "t.department" if "department" in prod_cols
        else ("t.dept" if "dept" in prod_cols else "NULL::VARCHAR")
    )
    DUCK.execute(f"""
      CREATE OR REPLACE VIEW products AS
      SELECT
        CAST(t.s_no AS VARCHAR)                                        AS s_no,
        t.contract_no                                                  AS contract_no,
        t.status_of_contract                                           AS status_of_contract,
        t.organization_type                                            AS organization_type,
        t.ministry                                                     AS ministry,
        {department_expr}                                              AS department,
        t.organization_name                                            AS organization_name,
        t.office_zone                                                  AS office_zone,
        t.buyer_designation                                            AS buyer_designation,
        t.buying_mode                                                  AS buying_mode,
        t.bid_number                                                   AS bid_number,
        CAST(t.contract_date AS DATE)                                  AS contract_date,
        TRY_CAST(REPLACE(t.total, ',', '') AS DOUBLE)                  AS total,
        t.item_desc                                                    AS product_name,
        t.brand                                                        AS product_brand,
        t.model                                                        AS product_model,
        TRY_CAST(REPLACE(t.qty, ',', '') AS DOUBLE)                    AS ordered_quantity,
        TRY_CAST(REPLACE(t.price, ',', '') AS DOUBLE)                  AS price,
        TRY_CAST(REPLACE(t.unit_price, ',', '') AS DOUBLE)             AS unit_price,
        t.row_sig                                                      AS row_sig,
        CAST(COALESCE(t.occ, 1) AS SMALLINT)                           AS occ,
        t.source_file                                                  AS source_file,
        t.ingested_at                                                  AS ingested_at
      FROM v_products_raw AS t;
    """)
    # =================== SERVICES (DuckDB) ===================
    _duck_attach_readonly(DUCK, DUCK_SERVICES, "svc")
    DUCK.execute(f"""
      CREATE OR REPLACE VIEW v_services_raw AS
      SELECT * FROM svc.main.{DUCK_SERVICES_TABLE};
    """)
    DUCK.execute("""
      CREATE OR REPLACE VIEW services AS
      SELECT
        CAST(t.s_no AS VARCHAR)            AS s_no,
        t.contract_no                      AS contract_no,
        t.status                           AS status_of_contract,
        t.organization_type                AS organization_type,
        t.ministry                         AS ministry,
        t.department                       AS department,
        t.organization_name                AS organization_name,
        t.office_zone                      AS office_zone,
        t.buyer_designation                AS buyer_designation,
        t.buying_mode                      AS buying_mode,
        t.bid_number                       AS bid_number,
        CAST(t.contract_date AS DATE)      AS contract_date,
        CAST(t.total AS DOUBLE)            AS total,
        NULL::VARCHAR                      AS product_name,
        NULL::VARCHAR                      AS product_brand,
        NULL::VARCHAR                      AS product_model,
        t.service_name                     AS service_name,
        t.service_category                 AS service_category,
        CAST(t.ordered_quantity AS DOUBLE) AS ordered_quantity,
        CAST(t.price AS DOUBLE)            AS price,
        CAST(t.unit_price AS DOUBLE)       AS unit_price,
        t.row_sig                          AS row_sig,
        CAST(COALESCE(t.occ,1) AS SMALLINT) AS occ,
        t.source_file                      AS source_file,
        t.imported_at                      AS imported_at
      FROM v_services_raw t;
    """)
    # =================== MINISTRY (DuckDB) ===================
    _duck_attach_readonly(DUCK, DUCK_MINISTRY, "cen")
    DUCK.execute(f"""
      CREATE OR REPLACE VIEW v_ministry_raw AS
      SELECT * FROM cen.main.{DUCK_MINISTRY_TABLE};
    """)
    DUCK.execute("""
      CREATE OR REPLACE VIEW ministry AS
      SELECT
        CAST(t.s_no AS VARCHAR)            AS s_no,
        t.contract_no                      AS contract_no,
        t.status                           AS status_of_contract,
        t.organization_type                AS organization_type,
        t.ministry                         AS ministry,
        t.department                       AS department,
        t.organization_name                AS organization_name,
        t.office_zone                      AS office_zone,
        t.buyer_designation                AS buyer_designation,
        t.buying_mode                      AS buying_mode,
        t.bid_number                       AS bid_number,
        CAST(t.contract_date AS DATE)      AS contract_date,
        CAST(t.total AS DOUBLE)            AS total,
        t.product_name                     AS product_name,
        t.product_brand                    AS product_brand,
        t.product_model                    AS product_model,
        t.service_name                     AS service_name,
        t.service_category                 AS service_category,
        CAST(t.ordered_quantity AS DOUBLE) AS ordered_quantity,
        CAST(t.price AS DOUBLE)            AS price,
        CAST(t.unit_price AS DOUBLE)       AS unit_price,
        t.row_sig                          AS row_sig,
        CAST(COALESCE(t.occ,1) AS SMALLINT) AS occ,
        t.source_file                      AS source_file,
        t.imported_at                      AS imported_at
      FROM v_ministry_raw t;
    """)
    # =================== STATE (DuckDB) ===================
    _duck_attach_readonly(DUCK, DUCK_STATE, "sta")
    DUCK.execute(f"""
      CREATE OR REPLACE VIEW v_state_raw AS
      SELECT * FROM sta.main.{DUCK_STATE_TABLE};
    """)
    DUCK.execute("""
      CREATE OR REPLACE VIEW state AS
      SELECT
        CAST(t.s_no AS VARCHAR)            AS s_no,
        t.contract_no                      AS contract_no,
        t.status                           AS status_of_contract,
        t.organization_type                AS organization_type,
        t.state                            AS state,
        t.department                       AS department,
        t.organization_name                AS organization_name,
        t.office_zone                      AS office_zone,
        t.buyer_designation                AS buyer_designation,
        t.buying_mode                      AS buying_mode,
        t.bid_number                       AS bid_number,
        CAST(t.contract_date AS DATE)      AS contract_date,
        CAST(t.total AS DOUBLE)            AS total,
        t.product_name                     AS product_name,
        t.product_brand                    AS product_brand,
        t.product_model                    AS product_model,
        t.service_name                     AS service_name,
        t.service_category                 AS service_category,
        CAST(t.ordered_quantity AS DOUBLE) AS ordered_quantity,
        CAST(t.price AS DOUBLE)            AS price,
        CAST(t.unit_price AS DOUBLE)       AS unit_price,
        t.row_sig                          AS row_sig,
        CAST(COALESCE(t.occ,1) AS SMALLINT) AS occ,
        t.source_file                      AS source_file,
        t.imported_at                      AS imported_at
      FROM v_state_raw t;
    """)
# ========= JSON MOUNTS (matches the 8 files you shared) =========
# ---- replace your existing mount_json_views() with this hardened version ----
def mount_json_views():
    # central_portals.json → central_portals_v
    try:
        cp = json.load(open(_json_path("central_portals.json"), "r", encoding="utf-8"))
        rows_in = _get_list(cp, "portals")
        out = []
        for r in rows_in:
            out.append({
                "portal_id": r.get("portal_id"),
                "name": r.get("name"),
                "url": r.get("url"),
                "org_scope": r.get("org_scope"),
                "notes": r.get("notes")
            })
        _safe_register_json_rows(out, "central_portals_v",
                                 ["portal_id","name","url","org_scope","notes"])
        print(f"[json] mounted view central_portals_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] central_portals mount skipped: {e}")
    # state_portals.json → state_portals_v
    try:
        sp = json.load(open(_json_path("state_portals.json"), "r", encoding="utf-8"))
        rows_in = _get_list(sp, "portals")
        out = []
        for r in rows_in:
            out.append({
                "portal_id": r.get("portal_id"),
                "name": r.get("name"),
                "state_code": r.get("state_code"),
                "url": r.get("url"),
                "notes": r.get("notes")
            })
        _safe_register_json_rows(out, "state_portals_v",
                                 ["portal_id","name","state_code","url","notes"])
        print(f"[json] mounted view state_portals_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] state_portals mount skipped: {e}")
    # ministry_metadata.json → ministry_meta_v
    try:
        mm = json.load(open(_json_path("ministry_metadata.json"), "r", encoding="utf-8"))
        rows_in = _get_list(mm, "ministries")
        out = [{"ministry": r.get("ministry"), "abbr": r.get("abbr"), "notes": r.get("notes")}
               for r in rows_in]
        _safe_register_json_rows(out, "ministry_meta_v", ["ministry","abbr","notes"])
        print(f"[json] mounted view ministry_meta_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] ministry_metadata mount skipped: {e}")
    # states_metadata.json → states_meta_v
    try:
        sm = json.load(open(_json_path("states_metadata.json"), "r", encoding="utf-8"))
        rows_in = _get_list(sm, "states")
        out = [{"state_code": r.get("state_code"),
                "state_name": r.get("state_name"),
                "zone": r.get("zone")} for r in rows_in]
        _safe_register_json_rows(out, "states_meta_v", ["state_code","state_name","zone"])
        print(f"[json] mounted view states_meta_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] states_metadata mount skipped: {e}")
    # ministry_hierarchy.json → ministry_hierarchy_v
    try:
        mh = json.load(open(_json_path("ministry_hierarchy.json"), "r", encoding="utf-8"))
        nodes = _get_list(mh, "tree")
        out = []
        for t in nodes:
            ministry = t.get("ministry")
            deps = t.get("departments") or []
            if isinstance(deps, list) and deps:
                for d in deps:
                    out.append({"ministry": ministry, "department": d.get("department")})
            else:
                out.append({"ministry": ministry, "department": None})
        _safe_register_json_rows(out, "ministry_hierarchy_v", ["ministry","department"])
        print(f"[json] mounted view ministry_hierarchy_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] ministry_hierarchy mount skipped: {e}")
    # states_hierarchy.json → states_hierarchy_v
    try:
        sh = json.load(open(_json_path("states_hierarchy.json"), "r", encoding="utf-8"))
        nodes = _get_list(sh, "tree")
        out = []
        for t in nodes:
            state_code = t.get("state_code")
            state_name = t.get("state_name")
            deps = t.get("top_departments") or []
            if isinstance(deps, list) and deps:
                for d in deps:
                    out.append({"state_code": state_code,
                                "state_name": state_name,
                                "department": d.get("department")})
            else:
                out.append({"state_code": state_code, "state_name": state_name, "department": None})
        _safe_register_json_rows(out, "states_hierarchy_v", ["state_code","state_name","department"])
        print(f"[json] mounted view states_hierarchy_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] states_hierarchy mount skipped: {e}")
    # product_categories_metadata.json → product_categories_v
    try:
        pc = json.load(open(_json_path("product_categories_metadata.json"), "r", encoding="utf-8"))
        rows_in = _get_list(pc, "categories")
        out = []
        for r in rows_in:
            avail = r.get("available") or {}
            out.append({
                "category_id": r.get("category_id"),
                "name": r.get("name"),
                "slug": r.get("slug"),
                "rows_count": r.get("rows_count"),
                "available_min": avail.get("min"),
                "available_max": avail.get("max"),
                "updated_at": r.get("updated_at")
            })
        _safe_register_json_rows(out, "product_categories_v",
                                 ["category_id","name","slug","rows_count","available_min","available_max","updated_at"])
        print(f"[json] mounted view product_categories_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] product_categories mount skipped: {e}")
    # service_categories_metadata.json → service_categories_v
    try:
        sc = json.load(open(_json_path("service_categories_metadata.json"), "r", encoding="utf-8"))
        rows_in = _get_list(sc, "categories")
        out = []
        for r in rows_in:
            avail = r.get("available") or {}
            out.append({
                "service_category_id": r.get("service_category_id") or r.get("category_id"),
                "name": r.get("name"),
                "slug": r.get("slug"),
                "rows_count": r.get("rows_count"),
                "available_min": avail.get("min"),
                "available_max": avail.get("max"),
                "updated_at": r.get("updated_at")
            })
        _safe_register_json_rows(out, "service_categories_v",
                                 ["service_category_id","name","slug","rows_count","available_min","available_max","updated_at"])
        print(f"[json] mounted view service_categories_v ({len(out)} rows)")
    except Exception as e:
        print(f"[json] service_categories mount skipped: {e}")

# ========= BOOT =========
mount_sql_sources()
mount_json_views()
# ========= API =========
@app.get("/health")
def health():
    return {"ok": True, "ts": dt.datetime.utcnow().isoformat()}
@app.get("/tables")
def tables():
    df = DUCK.execute("PRAGMA show_tables;").df()
    return {"ok": True, "data": df.to_dict(orient="records")}
@app.get("/schema/{view}")
def schema(view: str):
    try:
        df = DUCK.execute(f"DESCRIBE {view};").df()
        return {"ok": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"ok": False, "error": str(e)}
@app.get("/sample/{view}")
def sample(view: str, n: int = 20):
    try:
        df = DUCK.execute(f"SELECT * FROM {view} LIMIT {int(n)};").df()
        return {"ok": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"ok": False, "error": str(e)}
# Ad-hoc SQL on in-memory DUCK
@app.post("/duck")
def run_duck(inp: SqlIn):
    try:
        df = DUCK.execute(inp.sql).df()
        return {"ok": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"ok": False, "error": str(e)}
# Named DB runners
@app.post("/duck2")
def run_duck_named(inp: SqlNamedIn):
    path = {"services": DUCK_SERVICES, "ministry": DUCK_MINISTRY, "state": DUCK_STATE}.get(inp.db)
    if not path or not os.path.exists(path):
        return {"ok": False, "error": f"unknown duckdb '{inp.db}'"}
    try:
        con = duckdb.connect(path, read_only=True)
        df = con.execute(inp.sql).df()
        con.close()
        return {"ok": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"ok": False, "error": str(e)}
@app.post("/sqlite2")
def run_sqlite_named(inp: SqlNamedIn):
    if inp.db != "products" or not os.path.exists(SQLITE_PRODUCTS):
        return {"ok": False, "error": "unknown sqlite 'products'"}
    try:
        con = sqlite3.connect(SQLITE_PRODUCTS)
        df = pd.read_sql_query(inp.sql, con)
        con.close()
        return {"ok": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"ok": False, "error": str(e)}
# JSON helpers
@app.get("/json/list")
def list_json():
    try:
        files = sorted([p.name for p in pathlib.Path(JSON_DIR).glob("*.json")])
        return {"ok": True, "data": files}
    except Exception as e:
        return {"ok": False, "error": str(e)}
def _deep_get(obj, dotted):
    cur = obj
    for part in dotted.split("."):
        if isinstance(cur, list):
            try:
                idx = int(part)
                cur = cur[idx]
            except Exception:
                return None
        else:
            cur = cur.get(part) if isinstance(cur, dict) else None
        if cur is None:
            break
    return cur
@app.post("/json/get")
def get_json(inp: JsonGetIn):
    p = pathlib.Path(JSON_DIR, f"{inp.name}.json")
    if not p.exists():
        return {"ok": False, "error": f"json '{inp.name}' not found in {JSON_DIR}"}
    data = json.load(open(p, "r", encoding="utf-8"))
    if inp.key:
        data = _deep_get(data, inp.key)
    return {"ok": True, "data": data}
# Excel passthrough
@app.post("/excel")
def load_excel(inp: ExcelIn):
    df = pd.read_excel(EXCEL_PATH, sheet_name=inp.sheet)
    return {"ok": True, "data": df.to_dict(orient="records")}
# BM25 (optional)
def bm25_search(dbfile, q, k):
    con = sqlite3.connect(dbfile)
    rows = con.execute("""
      SELECT d.id, d.path, d.title,
             snippet(docs_fts, 0, '[', ']', '...', 12) AS snip
      FROM docs_fts
      JOIN docs d ON d.id = docs_fts.rowid
      WHERE docs_fts MATCH ?
      ORDER BY rank LIMIT ?;
    """, (q, k)).fetchall()
    con.close()
    return [{"id": r[0], "path": r[1], "title": r[2], "snippet": r[3]} for r in rows]
@app.post("/bm25")
def search(inp: QIn):
    db = BM25_DB.get(inp.index)
    if not db:
        return {"ok": False, "error": "unknown index"}
    if not os.path.exists(db):
        return {"ok": False, "error": f"index db not found: {db}"}
    hits = bm25_search(db, inp.q, inp.k)
    return {"ok": True, "data": hits, "cite": [{"id": h["id"], "path": h["path"]} for h in hits]}
