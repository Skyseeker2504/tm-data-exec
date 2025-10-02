# startup.py
import os, pathlib
from index_builder import walk_and_index

os.makedirs("indices", exist_ok=True)

TEXT_ROOT  = os.getenv("TEXT_ROOT",  "E:/data_growth_agent/texts")
PPT_ROOT   = os.getenv("PPT_ROOT",   "E:/data_growth_agent/ppt")
MEDIA_ROOT = os.getenv("MEDIA_ROOT", "E:/data_growth_agent/media_txt")

targets = [
    ("indices/text_bm25.sqlite",  TEXT_ROOT,  "text"),
    ("indices/ppt_bm25.sqlite",   PPT_ROOT,   "ppt"),
    ("indices/media_bm25.sqlite", MEDIA_ROOT, "media"),
]

for dbfile, root, kind in targets:
    if not os.path.exists(dbfile):
        pathlib.Path(root).mkdir(parents=True, exist_ok=True)
        print(f"[startup] Building {dbfile} from {root} ({kind})")
        walk_and_index(dbfile, root, kind=kind)
    else:
        print(f"[startup] Found existing {dbfile}")
