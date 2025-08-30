from pathlib import Path
import json, re
from string import Template

class CTemplate(Template):
    delimiter = '§'

DATA_PATH = "data/test centers_with_google_maps_and_website_information.json"
TEMPLATE_PATH = "generate_professors_and_centers_files/center_template.qmd"
OUT_DIR = Path("centers")

def clean_pid(pid: str) -> str:
    pid = (pid or "").strip()
    pid = re.sub(r"[^A-Za-z0-9_-]", "-", pid)
    pid = re.sub(r"-{2,}", "-", pid).lower().strip("-")
    return pid or "x"

def yaml_escape(s: str) -> str:
    # escape for double-quoted YAML scalars
    return (s or "").replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ").strip()

with open(DATA_PATH, "r", encoding="utf-8") as f:
    centers = json.load(f)

with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
    center_template = CTemplate(f.read())

OUT_DIR.mkdir(parents=True, exist_ok=True)

seen = set()
def unique_slug(base: str) -> str:
    s, i = base, 2
    while s in seen:
        s = f"{base}-{i}"; i += 1
    seen.add(s); return s

generated, skipped = 0, 0
for c in centers:
    pid = c.get("place_id")
    if not pid:
        skipped += 1
        continue

    slug = unique_slug(clean_pid(pid))
    filename = OUT_DIR / f"{slug}.qmd"

    raw_title = c.get("name", slug)
    safe_title = yaml_escape(raw_title)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(center_template.substitute(title=safe_title, id=pid))
    generated += 1

print(f"Generated {generated} files; skipped {skipped} without place_id.")
