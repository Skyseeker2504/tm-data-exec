import os, pathlib
from index_builder import walk_and_index

os.makedirs("indices", exist_ok=True)

targets = [
    ("indices/text_bm25.sqlite",  "docs/text",  "text"),
    ("indices/ppt_bm25.sqlite",   "docs/ppt",   "ppt"),
    ("indices/media_bm25.sqlite", "docs/media", "media"),
]

for dbfile, root, kind in targets:
    if not os.path.exists(dbfile):
        pathlib.Path(root).mkdir(parents=True, exist_ok=True)
        print(f"[startup] Building {dbfile} from {root} ({kind})")
        walk_and_index(dbfile, root, kind=kind)
    else:
        print(f"[startup] Found existing {dbfile}")
