# SQL Practice Lab for Analytics Engineering (Databricks Free Edition)

A hands-on lab to build real SQL skills for **Analytics Engineer / dbt Developer** roles: a realistic e-commerce
dataset (14 tables, ~700k rows), **250 practice questions with hints but no answers**, a concepts reference and a progress tracker.

## Use the files in this order

| Step | File | What to do |
|---|---|---|
| 1 | [`01_setup_ecommerce_lab.py`](01_setup_ecommerce_lab.py) | Run once in Databricks. Builds schema `workspace.ecommerce_lab` with all tables and data (deterministic, safe to re-run; ~1–2 min on a 4-core local Spark, expect a few minutes on Serverless). A notebook copy is provided as [`01_setup_ecommerce_lab.ipynb`](01_setup_ecommerce_lab.ipynb) — use whichever imports more easily. |
| 2 | [`DATA_QUALITY_ISSUES.md`](DATA_QUALITY_ISSUES.md) | **Optional, spoilers.** Documents the intentionally planted data problems. Read it *after* you attempt Level 8 if you want to discover them yourself. |
| 3 | [`03_sql_analytics_engineering_reference.md`](03_sql_analytics_engineering_reference.md) | Skim before you start; come back whenever a concept (windows, joins, SCDs, incremental logic…) feels shaky. Contains no answers to the 250 questions. |
| 4 | [`02_sql_practice_250_questions.ipynb`](02_sql_practice_250_questions.ipynb) | Import into Databricks and solve the questions **in order**. Each question = scenario/task cell + hint cell + empty `%sql` cell. |
| 5 | [`04_sql_learning_tracker.md`](04_sql_learning_tracker.md) | Tick off attempted / solved / needed hints / revisit as you go. |

## Quick start on Databricks Free Edition

1. **Import the setup script:** *Workspace → (your folder) → Import →* choose `01_setup_ecommerce_lab.py`
   (the first line `# Databricks notebook source` makes Databricks open it as a notebook with proper cells).
2. Attach **Serverless** compute → **Run all**. The last cells print row counts, integrity checks and a few sanity queries.
3. **Import the practice notebook** `02_sql_practice_250_questions.ipynb` the same way.
4. Run the two *Connect to the lab* cells, then start at **Question 1**. Every exercise cell begins with `%sql`; write your query below the comment and press **Shift + Enter**.
5. Use the notebook outline (list icon in the left sidebar) to jump between the 10 levels and individual questions.

> Data snapshot date is **2025-12-31**. Never use `current_date()` in the exercises — answers must be reproducible.

## What is in the dataset

| Core tables | Extra tables (for analytics-engineering exercises) |
|---|---|
| `customers` (12,000) · `orders` (60k) · `order_items` (140k) · `products` (1,200) · `categories` (47) · `payments` (63k) · `returns` (8k) · `inventory` (6k) · `warehouses` (12) · `customer_activity` (160k) | `order_status_history` (event log) · `customer_changes` (CDC feed for SCD exercises) · `dim_date` (calendar with fiscal year & holidays) · `orders_incoming_batch` (next incremental load) |

Core relationships are valid (no orphan orders, items, payments or returns) while the *content* is deliberately messy: NULLs,
duplicates, inconsistent casing, stray spaces, late-arriving rows, failed/refunded payments, discontinued products, stuck orders, and more.

## The 250 questions

| Level | Section | Questions |
|---|---|---|
| 1 | SQL Fundamentals | 1–25 |
| 2 | Aggregations and Grouping | 26–50 |
| 3 | Joins and Relationships | 51–80 |
| 4 | Subqueries and CTEs | 81–105 |
| 5 | Window Functions | 106–140 |
| 6 | Date and Time Analytics | 141–160 |
| 7 | Advanced Analytical SQL | 161–190 |
| 8 | Data Cleaning and Data Quality | 191–210 |
| 9 | Data Warehousing & Analytics Engineering SQL | 211–230 |
| 10 | Advanced Business Scenarios & Interview Challenges | 231–250 |

Difficulty labels: Beginner (11) · Easy (23) · Intermediate (64) · Advanced (103) · Expert (49). Difficulty rises within every level.

### Suggested way of working
* Read the scenario, write the query yourself, run it, then **sanity-check the numbers** (row counts after joins, totals vs a simple query).
* Only open the hint after a real attempt; log it in the tracker.
* Do Levels 1–4 in roughly a week, Level 5 slowly (it is the most important), Level 9 with a dbt mindset: each question is the SQL core of a staging model, dimension, fact, snapshot or incremental model you could later build as real dbt models in this repository.
* Re-solve every Expert question you needed help with after a week, without hints.

## Compatibility and safety
* Databricks SQL / Spark SQL only; Delta tables; Serverless-compatible (no RDDs, no `sparkContext`).
* The setup only drops and recreates tables **inside `ecommerce_lab`**; it creates temporary `_gen_*` helper tables and removes them at the end.
* Set `CATALOG`/`SCHEMA` at the top of the script (or the `LAB_CATALOG` / `LAB_SCHEMA` environment variables) to build elsewhere.
* If your workspace has no `workspace` catalog the script falls back to the current default catalog.
* Primary/foreign keys are declared as *informational* constraints (not enforced). If your workspace rejects them, the script reports it and continues.
* `WITH RECURSIVE` (used optionally in Q96) needs a recent runtime; the question also works with chained self-joins.

## Validation status (what was actually tested)

| Check | Result |
|---|---|
| Setup script executed end-to-end on **Spark 4.0.1** (local), with both Parquet and **Delta Lake 4.0** tables | ✅ passes; all minimum row counts met; core integrity checks return 0 orphans |
| Determinism: two independent full runs (Parquet and Delta) | ✅ identical checksums on six tables |
| Intended data-quality issues present at expected rates | ✅ verified with SQL |
| **198 throw-away reference queries** (not included in this repository) run against the generated data, covering question types in all 10 levels | ✅ all execute without error and return rows |
| 250 questions: count, unique titles/tasks, difficulty never decreases within a level, all referenced tables/columns exist, every question has a hint, no SQL text in questions/hints | ✅ `python sql/tools/validate_outputs.py` |
| Notebook is valid `nbformat` 4 JSON; 250 × (question, hint, empty SQL cell); no answer cells or files | ✅ same validator |
| **Not tested here:** execution on real Databricks Free Edition compute; informational PK/FK declaration on Unity Catalog; `WITH RECURSIVE` (not supported by the local Spark 4.0) | ⚠️ expected to work, please report any error |

## Repository layout

```
sql/
├── README.md                                   <- you are here
├── 01_setup_ecommerce_lab.py                   <- Databricks notebook-source script (run first)
├── 01_setup_ecommerce_lab.ipynb                <- same setup as an .ipynb notebook (generated)
├── 02_sql_practice_250_questions.ipynb         <- the exercises (no answers)
├── 03_sql_analytics_engineering_reference.md   <- concept reference
├── 04_sql_learning_tracker.md                  <- progress checklist
├── DATA_QUALITY_ISSUES.md                      <- planted data problems (spoilers)
└── tools/                                      <- generators + validators (question text only, no solutions)
    ├── q_level*.py                             <- the question bank, source of truth for notebook and tracker
    ├── build_lab_files.py                      <- builds the notebook + tracker, validates the bank
    ├── build_setup_notebook.py                 <- converts the setup .py to .ipynb
    └── validate_outputs.py                     <- validates generated files
```

To regenerate after editing questions: `python3 sql/tools/build_lab_files.py && python3 sql/tools/validate_outputs.py`
(requires `pip install nbformat`).
