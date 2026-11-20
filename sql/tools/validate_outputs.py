#!/usr/bin/env python3
"""Validates the generated practice notebook, tracker and the absence of solutions. Exit code 1 on any failure."""
import json, os, re, sys
import nbformat
HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.dirname(HERE)
fails = []
def check(cond, msg):
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond: fails.append(msg)

# ---------- practice notebook ----------
nb = nbformat.read(os.path.join(OUT, "02_sql_practice_250_questions.ipynb"), as_version=4)
try:
    nbformat.validate(nb); valid = True
except Exception as e:
    valid = False; print(e)
check(valid, "practice notebook is valid nbformat 4 JSON")
ids = [c["id"] for c in nb.cells]
check(len(ids) == len(set(ids)), "all cell ids unique")
q_cells = [c for c in nb.cells if c.id.endswith("-question")]
h_cells = [c for c in nb.cells if c.id.endswith("-hint")]
s_cells = [c for c in nb.cells if c.id.endswith("-sql")]
check(len(q_cells) == len(h_cells) == len(s_cells) == 250, f"exactly 250 question / hint / sql cells ({len(q_cells)}/{len(h_cells)}/{len(s_cells)})")
nums = [int(re.search(r"### Question (\d+):", c.source).group(1)) for c in q_cells]
check(nums == list(range(1, 251)), "question numbers are 1..250 in order")
order_ok = True
q_index = {c.id[:4]: i for i, c in enumerate(nb.cells) if c.id.endswith("-question")}
for n in range(1, 251):
    k = f"q{n:03d}"
    i = q_index[k]
    if not (nb.cells[i].id == f"{k}-question" and nb.cells[i+1].id == f"{k}-hint" and nb.cells[i+2].id == f"{k}-sql"):
        order_ok = False
check(order_ok, "every question is followed immediately by its hint then its SQL cell")
empty_ok = True
for n, c in enumerate(s_cells, 1):
    lines = [l for l in c.source.split("\n") if l.strip()]
    if not (lines[0] == "%sql" and len(lines) == 2 and lines[1].startswith("-- Question") and "SELECT" not in c.source.upper()
            and c.outputs == [] and c.cell_type == "code"):
        empty_ok = False; print("   bad cell", c.id, lines)
check(empty_ok, "all 250 SQL exercise cells contain only '%sql' and one comment line (no solution)")
check(all(c.source.startswith("💡 **Hint") for c in h_cells), "all hint cells are markdown hints")
levels = [c for c in nb.cells if c.id.startswith("level-")]
check(len(levels) == 10, "10 level section headers present")
diffs = {}
for c in q_cells:
    d = re.search(r"\*\*Difficulty:\*\* (\w+)", c.source).group(1); diffs[d] = diffs.get(d, 0) + 1
check(set(diffs) <= {"Beginner", "Easy", "Intermediate", "Advanced", "Expert"} and sum(diffs.values()) == 250, f"difficulty labels valid {diffs}")
text = json.dumps(nb)
check("hidden" not in text.lower() or "hidden answer" not in text.lower(), "no hidden-answer cells")
check(not any(re.search(r"\bsolution\s*:", c.source, re.I) for c in nb.cells if c.cell_type == "markdown" and "-question" in c.id), "no 'Solution:' sections in question cells")
# SQL-looking snippets in question/hint markdown that look like a full query
leak = [c.id for c in q_cells + h_cells if re.search(r"\bSELECT\b.*\bFROM\b", re.sub(r"\*\*Topic:\*\*[^|\n]*", "", c.source), re.I | re.S)]
check(not leak, f"no SELECT...FROM query text in any question/hint cell {leak}")

# ---------- tracker ----------
tr = open(os.path.join(OUT, "04_sql_learning_tracker.md"), encoding="utf-8").read()
rows = re.findall(r"^\| (\d+) \| .+ \| \[ \] \| \[ \] \| \[ \] \| \[ \] \|$", tr, re.M)
check([int(x) for x in rows] == list(range(1, 251)), "tracker has 250 checklist rows numbered 1..250")
check("| **Total** | **250** |" in tr, "tracker difficulty summary totals 250")

# ---------- no answer files / no solutions elsewhere ----------
bad_names = [f for f in os.listdir(OUT) if re.search(r"(answer|solution|key)", f, re.I)]
check(not bad_names, f"no answer/solution/key files in the sql folder {bad_names}")
for fn in sorted(os.listdir(OUT)):
    if fn.endswith(".ipynb") and fn.startswith("01_"):
        n01 = nbformat.read(os.path.join(OUT, fn), as_version=4); nbformat.validate(n01)
        check(True, f"{fn} is valid nbformat ({len(n01.cells)} cells)")
sys.exit(1 if fails else 0)
