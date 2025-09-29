from fastapi import FastAPI
from pydantic import BaseModel
import duckdb, pandas as pd, sqlite3, os

app = FastAPI()
DUCK = duckdb.connect(database=':memory:')

EXCEL_PATH = os.getenv("EXCEL_PATH", "docs/gem_categories.xlsx")
BM25_DB = {
    "text":  "indices/text_bm25.sqlite",
    "ppt":   "indices/ppt_bm25.sqlite",
    "media": "indices/media_bm25.sqlite"
}

class SqlIn(BaseModel):
    sql: str

class ExcelIn(BaseModel):
    sheet: str

class QIn(BaseModel):
    q: str
    k: int = 8
    index: str = "text"

@app.post("/duck")
def run_duck(inp: SqlIn):
    try:
        df = DUCK.execute(inp.sql).df()
        return {"ok": True, "data": df.to_dict(orient="records")}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/excel")
def load_excel(inp: ExcelIn):
    df = pd.read_excel(EXCEL_PATH, sheet_name=inp.sheet)
    return {"ok": True, "data": df.to_dict(orient="records")}

def bm25_search(dbfile, q, k):
    con = sqlite3.connect(dbfile)
    cur = con.cursor()
    rows = cur.execute("""
      SELECT d.id, d.path, d.title,
             snippet(docs_fts, 0, '[', ']', '...', 12) AS snip
      FROM docs_fts
      JOIN docs d ON d.id = docs_fts.rowid
      WHERE docs_fts MATCH ?
      ORDER BY rank
      LIMIT ?;
    """, (q, k)).fetchall()
    con.close()
    return [{"id": r[0], "path": r[1], "title": r[2], "snippet": r[3]} for r in rows]

@app.post("/bm25")
def search(inp: QIn):
    db = BM25_DB.get(inp.index)
    if not db: return {"ok": False, "error": "unknown index"}
    hits = bm25_search(db, inp.q, inp.k)
    return {"ok": True, "data": hits, "cite": [{"id": h["id"], "path": h["path"]} for h in hits]}
