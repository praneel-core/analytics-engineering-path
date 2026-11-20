#!/usr/bin/env python3
"""Converts 01_setup_ecommerce_lab.py (Databricks 'notebook source' format) into 01_setup_ecommerce_lab.ipynb."""
import json, os, re
HERE = os.path.dirname(os.path.abspath(__file__)); OUT = os.path.dirname(HERE)
src = open(os.path.join(OUT, "01_setup_ecommerce_lab.py"), encoding="utf-8").read()
src = src.replace("# Databricks notebook source\n", "", 1)
chunks = [c.strip("\n") for c in re.split(r"\n# COMMAND ----------\n", src)]
cells = []
for i, chunk in enumerate(chunks):
    lines = chunk.split("\n")
    if lines and all(l.startswith("# MAGIC") or not l.strip() for l in lines):
        md = [re.sub(r"^# MAGIC ?", "", l) for l in lines]
        if md and md[0].strip() == "%md":
            md = md[1:]
        cells.append({"cell_type": "markdown", "id": f"setup-{i:02d}", "metadata": {}, "source": [l + "\n" for l in md[:-1]] + [md[-1]]})
    else:
        cells.append({"cell_type": "code", "id": f"setup-{i:02d}", "metadata": {}, "execution_count": None, "outputs": [],
                      "source": [l + "\n" for l in lines[:-1]] + [lines[-1]]})
nb = {"cells": cells,
      "metadata": {"application/vnd.databricks.v1+notebook": {"notebookName": "01_setup_ecommerce_lab", "language": "python", "dashboards": [], "widgets": {}},
                   "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}, "language_info": {"name": "python"}},
      "nbformat": 4, "nbformat_minor": 5}
path = os.path.join(OUT, "01_setup_ecommerce_lab.ipynb")
json.dump(nb, open(path, "w", encoding="utf-8"), indent=1, ensure_ascii=False)
print(f"wrote {path}: {len(cells)} cells ({sum(c['cell_type']=='code' for c in cells)} code)")
