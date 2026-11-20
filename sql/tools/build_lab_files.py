#!/usr/bin/env python3
"""
Builds the practice notebook and the progress tracker from the question bank, and validates the bank.

    python3 sql/tools/build_lab_files.py

Inputs : sql/tools/q_level*.py   (questions only - there are NO solutions anywhere in this project)
Outputs: sql/02_sql_practice_250_questions.ipynb
         sql/04_sql_learning_tracker.md
"""
import json
import os
import re
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import q_level01_02 as a  # noqa: E402
import q_level03_04 as b  # noqa: E402
import q_level05_06 as c  # noqa: E402
import q_level07_08 as d  # noqa: E402
import q_level09_10 as e  # noqa: E402

CATALOG, SCHEMA = "workspace", "ecommerce_lab"
DIFF_ORDER = {"Beginner": 0, "Easy": 1, "Intermediate": 2, "Advanced": 3, "Expert": 4}

LEVELS = [
    (1, "SQL Fundamentals", a.LEVEL_1, "SELECT, WHERE, ORDER BY, LIMIT, DISTINCT, aliases, comparison and logical operators, BETWEEN, IN, LIKE, IS NULL, CASE WHEN, date filtering, calculations"),
    (2, "Aggregations and Grouping", a.LEVEL_2, "COUNT, SUM, AVG, MIN, MAX, GROUP BY, HAVING, multi-column grouping, conditional aggregation, COUNT DISTINCT"),
    (3, "Joins and Relationships", b.LEVEL_3, "INNER / LEFT / RIGHT / FULL / CROSS / SELF joins, multi-table joins, unmatched records, cardinality and fan-out"),
    (4, "Subqueries and CTEs", b.LEVEL_4, "scalar, correlated and nested subqueries, CTEs, EXISTS / NOT EXISTS, IN / NOT IN, derived tables, hierarchies"),
    (5, "Window Functions", c.LEVEL_5, "ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST/LAST_VALUE, running totals, moving averages, frames, deduplication, top-N per group"),
    (6, "Date and Time Analytics", c.LEVEL_6, "date functions, truncation, differences, weeks / quarters, rolling windows, business days, period comparisons"),
    (7, "Advanced Analytical SQL", d.LEVEL_7, "CLV, retention, cohorts, growth, market basket, funnels, Pareto, segmentation, percentile analysis"),
    (8, "Data Cleaning and Data Quality", d.LEVEL_8, "duplicates, NULLs, COALESCE / NULLIF, string functions, regex, standardisation, integrity and consistency checks"),
    (9, "Data Warehousing and Analytics Engineering SQL", e.LEVEL_9, "grain, facts, dimensions, surrogate keys, SCD 1/2, incremental logic, late-arriving data, reconciliation, dbt-style tests"),
    (10, "Advanced Business Scenarios and Interview Challenges", e.LEVEL_10, "multi-step, multi-concept business problems"),
]

SCHEMA_COLUMNS = {
    "customers": "customer_id first_name last_name email gender date_of_birth city state country signup_date customer_segment is_active updated_at",
    "orders": "order_id customer_id order_date order_status shipping_address payment_method total_amount discount_amount tax_amount shipping_amount updated_at loaded_at",
    "order_items": "order_item_id order_id product_id quantity unit_price discount line_total",
    "products": "product_id product_name category_id brand unit_cost selling_price launch_date is_active discontinued_date",
    "categories": "category_id category_name parent_category_id",
    "payments": "payment_id order_id payment_date payment_method payment_status payment_amount transaction_reference",
    "returns": "return_id order_item_id return_date return_reason returned_quantity refund_amount return_status",
    "inventory": "inventory_id product_id warehouse_id stock_quantity reserved_quantity reorder_level last_updated_at",
    "warehouses": "warehouse_id warehouse_name city country",
    "customer_activity": "activity_id customer_id activity_timestamp activity_type session_id device_type product_id",
    "order_status_history": "history_id order_id order_status status_timestamp changed_by",
    "customer_changes": "change_id customer_id change_timestamp received_at change_type city state customer_segment is_active",
    "dim_date": "date_key full_date year quarter month month_name week_of_year day_of_week day_name is_weekend is_holiday holiday_name fiscal_year fiscal_quarter",
    "orders_incoming_batch": "order_id customer_id order_date order_status shipping_address payment_method total_amount discount_amount tax_amount shipping_amount updated_at loaded_at batch_id",
}
SCHEMA_COLUMNS = {t: set(cols.split()) for t, cols in SCHEMA_COLUMNS.items()}
ALL_COLUMNS = set().union(*SCHEMA_COLUMNS.values())

# Backticked tokens in question text that are legitimate but are neither columns nor tables.
ALLOWED_TOKENS = {
    # derived / output column names asked for in tasks
    "margin_per_unit", "margin_pct", "price_band", "age", "age_band", "discount_pct", "total_customers", "customers_with_email",
    "total_shipping", "avg_order_value", "first_order", "last_order", "order_count", "revenue", "order_year", "order_quarter",
    "delivered_orders", "cancelled_orders", "returned_orders", "units_sold", "avg_unit_price", "avg_line_total", "orders_containing",
    "success_count", "failed_count", "cancel_rate_pct", "order_day", "orders_without_payment", "payments_without_order",
    "order_rows", "joined_rows", "distinct_orders_in_join", "attempts", "failed_attempts", "orders_placed", "item_revenue",
    "line_count", "units", "paid_amount", "payment_attempts", "customer_id_a", "customer_id_b", "available", "parent_name",
    "orders_with_both", "clean_products", "avg_valid_order", "diff_from_avg", "lifetime_spend", "evergreen_products",
    "refunded", "net_revenue", "item_revenue", "depth", "root_category", "pct_of_best_month", "share_pct", "total_spend",
    "order_seq", "unit_rank", "city_rank", "customer_count", "row_num", "rnk", "dense_rnk", "customer_total", "pct_of_customer_total",
    "cumulative_revenue", "daily_revenue", "orders_so_far", "category_avg_price", "previous_order_date", "days_since_previous",
    "next_order_date", "days_to_next_order", "is_last_order", "first_order_amount", "change_vs_first", "latest_status",
    "moving_avg_7d", "moving_avg_3m", "months_in_window", "monthly_revenue", "avg_prev_3_orders", "amount_vs_recent_avg",
    "trailing_7d_revenue", "rn", "keep", "rows_to_remove", "rank_in_category", "spend_rank", "spend_quartile", "min_spend",
    "max_spend", "avg_spend", "price_percent_rank", "price_cume_dist", "first_order_date", "last_order_date", "prev_month_revenue",
    "mom_growth_pct", "pct_of_total", "cumulative_pct", "max_gap_days", "gap_days", "new_customers", "cumulative_customers",
    "next_activity", "rank_this_month", "rank_prev_month", "rank_change", "avg_seconds_since_previous", "max_seconds_since_previous",
    "month_start", "week_start", "signup_year", "orders_2024", "orders_2025", "growth_pct", "tenure_days", "tenure_months",
    "calendar_date", "orders", "rolling_7d_orders", "fiscal_year", "signup_month", "activation_rate_pct", "payment_method",
    "order_hour", "revenue_same_month_last_year", "yoy_growth_pct", "month_number", "lifetime_value", "avg_order_value",
    "orders_from_new_customers", "orders_from_returning_customers", "gross_profit", "cost", "days_since_last_order",
    "cohort_month", "months_since_signup", "month_index", "active_customers", "retention_pct", "product_a", "product_b",
    "device", "pct_of_orders", "repeat_rate_pct", "activity_month", "add_to_cart_events", "decile", "pct_of_total_revenue",
    "conversion_pct", "buyers_within_30d", "cohort_size", "seasonality_index", "clean_city", "email_clean", "first_name_clean",
    "last_name_clean", "gender_clean", "segment_clean", "city_clean", "is_email_valid", "payment_method_clean", "payment_status_clean",
    "is_duplicate_callback", "product_name_clean", "brand_clean", "leaf_category", "mid_category", "customer_key_seq",
    "customer_key_hash", "order_date_key", "gross_amount", "discount_amount", "net_amount", "cost_amount", "order_total",
    "items_amount", "refunded_payment_amount", "refund_amount_returns", "valid_from", "valid_to", "is_current", "customer_key",
    "customer_name", "test_name", "failing_rows", "total_rows", "distinct_order_item_id", "distinct_order_product_pairs",
    "header_total", "lines_total", "difference", "orders_with_difference", "month_to_date_revenue", "net_clv", "global_rank",
    "country_rank", "second_order_date", "days_to_second_order", "rolling_30d_revenue", "quarter", "valid_revenue", "aov",
    "returning_order_share_pct", "cancel_rate_pct", "refund_rate_pct", "revenue_mom_growth_pct", "orders_count", "lifetime_revenue",
    "days_since_last_order", "returns_count", "refunded_amount", "distinct_categories", "activity_events_2025", "is_repeat_buyer",
    "is_never_bought", "units_returned", "revenue_rank_in_month", "revenue_change_vs_prev_month", "return_rate_pct",
    "never_purchased_pct", "avg_days_to_first_order", "median_days_to_first_order", "signup_quarter", "available_stock",
    "daily_velocity", "days_of_cover", "expected_net", "recon_status", "paid", "root_category", "month_start",
    "daily_revenue", "return_reason", "units_sold", "cumulative_pct", "is_active", "is_holiday", "customer_segment",
    "returned_quantity", "refund_amount", "price_percent_rank", "lifetime_spend", "first_order_amount",
    "monthly_revenue", "pct_of_total",
    # status / enumerations quoted as code
    "product_view", "add_to_cart", "checkout_start", "purchase",
    "distinct_customers", "warehouse_count", "sessions", "previous_status", "last_order_amount", "percentile_approx",
    "percentile", "rank", "fact_order_items",
}

# Reject text that looks like a pasted solution.
SOLUTION_PATTERNS = [r"\bSELECT\b.{0,80}\bFROM\b", r"\bJOIN\b\s+\w+\s+ON\b.*=", r"\bGROUP BY\b\s+\w+\s*,?\s*\w*\s+HAVING\b"]


def flatten():
    out, n = [], 0
    for lvl, name, items, topics in LEVELS:
        for item in items:
            n += 1
            title, topic, diff, scenario, task, tables, hint = item
            out.append(dict(n=n, level=lvl, level_name=name, title=title, topic=topic, difficulty=diff,
                            scenario=scenario, task=task, tables=tables, hint=hint))
    return out


def validate(qs):
    problems = []
    if len(qs) != 250:
        problems.append(f"expected 250 questions, found {len(qs)}")
    # numbering ranges
    expected = [(1, 25), (26, 50), (51, 80), (81, 105), (106, 140), (141, 160), (161, 190), (191, 210), (211, 230), (231, 250)]
    for (lvl, name, items, _), (lo, hi) in zip(LEVELS, expected):
        first = next(q for q in qs if q["level"] == lvl)["n"]
        last = [q for q in qs if q["level"] == lvl][-1]["n"]
        if (first, last) != (lo, hi):
            problems.append(f"Level {lvl} spans {first}-{last}, expected {lo}-{hi}")
    # uniqueness
    for key in ("title", "task"):
        cnt = Counter(q[key].strip().lower() for q in qs)
        for k, v in cnt.items():
            if v > 1:
                problems.append(f"duplicate {key}: {k[:60]}")
    # difficulty non-decreasing within each level
    for lvl, name, items, _ in LEVELS:
        seq = [DIFF_ORDER[q["difficulty"]] for q in qs if q["level"] == lvl]
        if seq != sorted(seq):
            problems.append(f"Level {lvl}: difficulty is not non-decreasing")
        if any(d not in DIFF_ORDER for d in [q["difficulty"] for q in qs if q["level"] == lvl]):
            problems.append(f"Level {lvl}: unknown difficulty label")
    # every field populated, hint is meaningful, tables and column tokens are real
    for q in qs:
        for f in ("title", "topic", "difficulty", "scenario", "task", "hint"):
            if not q[f] or not q[f].strip():
                problems.append(f"Q{q['n']}: empty {f}")
        if len(q["hint"].split()) < 8:
            problems.append(f"Q{q['n']}: hint too short")
        if not q["tables"]:
            problems.append(f"Q{q['n']}: no tables")
        for t in q["tables"]:
            if t not in SCHEMA_COLUMNS:
                problems.append(f"Q{q['n']}: unknown table {t}")
        text = q["task"] + " " + q["scenario"] + " " + q["hint"]
        for tok in re.findall(r"`([^`]+)`", text):
            if re.fullmatch(r"[a-z_0-9]+", tok) and tok not in ALL_COLUMNS and tok not in SCHEMA_COLUMNS and tok not in ALLOWED_TOKENS:
                problems.append(f"Q{q['n']}: backticked token `{tok}` is not a known column/table/output name")
        for pat in SOLUTION_PATTERNS:
            if re.search(pat, text, re.I | re.S):
                problems.append(f"Q{q['n']}: text looks like it contains a SQL solution ({pat})")
    # topic coverage sanity: each level has >= 1 question of each stated size
    return problems


# ------------------------------------------------------------------------------------------------
# Notebook
# ------------------------------------------------------------------------------------------------
INTRO_1 = f"""# 🎯 250-Question SQL Practice Lab — E-commerce Analytics Engineering

You work as an **Analytics Engineer** at a large online retailer. The business team keeps asking questions about
sales, customers, products, payments, returns and operations. Your job is to answer them **by writing SQL yourself**.

> ✅ **Nothing in this notebook gives away an answer.** Each question has a business scenario, a task, the tables
> to use and a hint that nudges your approach without writing the query for you.

## How to use this notebook
1. Run the setup script first: `01_setup_ecommerce_lab.py` (once; it can be re-run safely).
2. Run the two *Connect to the lab* cells below.
3. Work through the questions **in order**. Each question has three cells:
   * a question cell (scenario, task, tables, difficulty),
   * a **hint** cell — try the question first; open the hint only if you are stuck,
   * an empty **SQL cell** where you write your solution and press **Shift + Enter** (or the ▶ button) to run it.
4. Track your progress in `04_sql_learning_tracker.md`. Use `03_sql_analytics_engineering_reference.md` when you need to revise a concept.

## How SQL cells work in Databricks
Every exercise cell starts with the `%sql` magic so it runs as Databricks SQL / Spark SQL regardless of the notebook's
default language. Replace the comment with your query. You can write several statements in one cell, but only
the result of the last `SELECT` is displayed — keep one query per cell.

## Where the data lives
Catalog **`{CATALOG}`**, schema (database) **`{SCHEMA}`**. After the *Connect* cell you can use plain table names such as
`customers`; otherwise write `{CATALOG}.{SCHEMA}.customers`.

**Data snapshot date: `2025-12-31`.** Orders span 2022-01-01 → 2025-12-31. Treat 2025-12-31 as "today" and
**never use `current_date()`** — otherwise your answers will drift every day.
"""

INTRO_2 = """## The data model (14 tables)

| Table | Grain (one row per…) | Key columns |
|---|---|---|
| `customers` | customer record | `customer_id` |
| `orders` | order | `order_id`, `customer_id` → customers |
| `order_items` | order line | `order_item_id`, `order_id` → orders, `product_id` → products |
| `products` | product | `product_id`, `category_id` → categories |
| `categories` | category (3-level tree) | `category_id`, `parent_category_id` → categories |
| `payments` | payment attempt | `payment_id`, `order_id` → orders |
| `returns` | returned order line | `return_id`, `order_item_id` → order_items |
| `inventory` | product × warehouse stock | `inventory_id`, `product_id`, `warehouse_id` |
| `warehouses` | warehouse | `warehouse_id` |
| `customer_activity` | clickstream event | `activity_id`, `customer_id` (NULL = anonymous), `session_id`, `product_id` |
| `order_status_history` | status change of an order | `history_id`, `order_id` |
| `customer_changes` | change-feed row for a customer (for SCD exercises) | `change_id`, `customer_id` |
| `dim_date` | calendar day | `date_key`, `full_date` |
| `orders_incoming_batch` | row in the next incremental order load | `order_id` (not unique), `batch_id` |

```
customers ──< orders ──< order_items >── products >── categories (self-referencing tree)
                │              │              │
                │              └──< returns    └──< inventory >── warehouses
                ├──< payments
                └──< order_status_history
customers ──< customer_activity >── products          customers ──< customer_changes
dim_date (calendar)                    orders_incoming_batch (staging for orders)
```

## Business glossary used in the questions
* **Valid order** — `order_status` NOT IN ('Cancelled', 'Failed'). Whenever a question says *valid*, use this rule.
* **Order statuses** — Pending, Processing, Shipped, Delivered, Cancelled, Returned, Failed.
* **Item revenue** — `order_items.line_total` (after line discount, before tax and shipping). **Order revenue** — `orders.total_amount` (items + 18 % GST + shipping).
* **Refunded amount** — `returns.refund_amount` where `return_status` = 'Refunded'.
* **Payment statuses** — Success, Failed, Pending, Refunded. **Return statuses** — Pending, Approved, Rejected, Refunded.
* All money is in **INR**.
* The data is deliberately **messy** (just like production): duplicates, NULLs, inconsistent casing, stray spaces, late-arriving rows… Early questions say *"use the value as stored / exact match"*; from Level 8 onward you clean it properly. Read each task carefully for what is expected.

## Difficulty scale
`Beginner` → `Easy` → `Intermediate` → `Advanced` → `Expert`. Difficulty rises inside every level, and each level starts harder than the previous one.
"""


def build_toc(qs):
    rows = ["| # | Section | Questions | Focus |", "|---|---|---|---|"]
    for lvl, name, items, topics in LEVELS:
        first = next(q for q in qs if q["level"] == lvl)["n"]
        last = [q for q in qs if q["level"] == lvl][-1]["n"]
        rows.append(f"| {lvl} | [{name}](#level-{lvl}) | {first}–{last} ({len(items)}) | {topics} |")
    return "<a id=\"toc\"></a>\n## Table of contents\n\n" + "\n".join(rows) + \
        "\n\n*In Databricks, open the notebook outline (the list icon in the left sidebar) to jump straight to any section or question — every level and question title is a heading.*"


def md_cell(cell_id, text):
    return {"cell_type": "markdown", "id": cell_id, "metadata": {}, "source": text.splitlines(True)}


def code_cell(cell_id, text):
    return {"cell_type": "code", "id": cell_id, "metadata": {}, "execution_count": None, "outputs": [],
            "source": text.splitlines(True)}


def build_notebook(qs):
    cells = [md_cell("intro-1", INTRO_1), md_cell("intro-2", INTRO_2), md_cell("toc", build_toc(qs))]
    cells.append(md_cell("connect-md", "## 🔌 Connect to the lab\nRun these two cells once per session, then peek at the tables."))
    cells.append(code_cell("connect-py", f"# Point this notebook session at the practice schema\nspark.sql(\"USE CATALOG {CATALOG}\")\nspark.sql(\"USE SCHEMA {SCHEMA}\")\nprint(\"Connected to {CATALOG}.{SCHEMA}\")"))
    cells.append(code_cell("connect-tables", f"%sql\nSHOW TABLES IN {CATALOG}.{SCHEMA}"))
    cells.append(md_cell("connect-explore", "Handy commands while you work: `DESCRIBE TABLE orders;`, `DESCRIBE TABLE EXTENDED orders;`, `SELECT * FROM orders LIMIT 5;` — use them in your own scratch cells (add a cell with **+ Code** and start it with `%sql`)."))
    cells.append(code_cell("scratch", "%sql\n-- Scratch cell: explore the data here (not part of any question)\n"))
    seen_levels = set()
    for q in qs:
        if q["level"] not in seen_levels:
            seen_levels.add(q["level"])
            lvl, name, items, topics = LEVELS[q["level"] - 1]
            first = q["n"]
            last = [x for x in qs if x["level"] == lvl][-1]["n"]
            cells.append(md_cell(f"level-{lvl}", f"<a id=\"level-{lvl}\"></a>\n# Level {lvl}: {name}\n**Questions {first}–{last}** · {len(items)} questions\n\n*Topics:* {topics}\n\n[↑ Back to table of contents](#toc)"))
        n = q["n"]
        tables = ", ".join(f"`{t}`" for t in q["tables"])
        cells.append(md_cell(f"q{n:03d}-question",
                             f"### Question {n}: {q['title']}\n"
                             f"**Difficulty:** {q['difficulty']} &nbsp;|&nbsp; **Topic:** {q['topic']} &nbsp;|&nbsp; **Level {q['level']}**\n\n"
                             f"**Business scenario:** {q['scenario']}\n\n"
                             f"**Task:** {q['task']}\n\n"
                             f"**Tables:** {tables}"))
        cells.append(md_cell(f"q{n:03d}-hint", f"💡 **Hint (Question {n}):** {q['hint']}"))
        cells.append(code_cell(f"q{n:03d}-sql", f"%sql\n-- Question {n}: write your solution here\n"))
    cells.append(md_cell("done", "# 🎉 Finished?\nUpdate `04_sql_learning_tracker.md`, revisit every question you flagged, and re-solve the Expert questions without hints a week later. "
                                 "Then try rebuilding the Level 9 models as real dbt models in this repository."))
    nb = {
        "cells": cells,
        "metadata": {
            "application/vnd.databricks.v1+notebook": {"notebookName": "02_sql_practice_250_questions", "language": "python", "dashboards": [], "widgets": {}},
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return nb


# ------------------------------------------------------------------------------------------------
# Tracker
# ------------------------------------------------------------------------------------------------
def build_tracker(qs):
    diff_counts = Counter(q["difficulty"] for q in qs)
    lines = ["# SQL Learning Tracker — 250 Questions", "",
             "Copy this file or edit it in place. Replace `[ ]` with `[x]` as you go.", "",
             "| Column | Meaning |", "|---|---|",
             "| Attempted | You wrote a query and ran it |",
             "| Solved independently | Correct result without hints or looking anything up |",
             "| Needed hints | You opened the hint cell |",
             "| Revisit | Solve it again in a week (and always for anything you needed hints on) |", "",
             "## Summary by difficulty", "", "| Difficulty | Questions |", "|---|---:|"]
    for dname in DIFF_ORDER:
        lines.append(f"| {dname} | {diff_counts.get(dname, 0)} |")
    lines.append(f"| **Total** | **{len(qs)}** |")
    lines += ["", "## Summary by level", "",
              "| Level | Section | Questions | Beginner | Easy | Intermediate | Advanced | Expert | Solved (fill in) |", "|---|---|---|---:|---:|---:|---:|---:|---|"]
    for lvl, name, items, _ in LEVELS:
        lq = [q for q in qs if q["level"] == lvl]
        dc = Counter(q["difficulty"] for q in lq)
        lines.append(f"| {lvl} | {name} | {lq[0]['n']}–{lq[-1]['n']} ({len(lq)}) | " + " | ".join(str(dc.get(x, 0)) for x in DIFF_ORDER) + " | ___ / " + str(len(lq)) + " |")
    lines += ["", "## Progress dashboard (update manually)", "",
              "| Metric | Count |", "|---|---:|", "| Attempted | 0 / 250 |", "| Solved independently | 0 / 250 |",
              "| Needed hints | 0 |", "| To revisit | 0 |", ""]
    for lvl, name, items, topics in LEVELS:
        lq = [q for q in qs if q["level"] == lvl]
        lines += [f"## Level {lvl}: {name} (Questions {lq[0]['n']}–{lq[-1]['n']})", "",
                  "| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |",
                  "|---:|---|---|---|:---:|:---:|:---:|:---:|"]
        for q in lq:
            lines.append(f"| {q['n']} | {q['title']} | {q['topic']} | {q['difficulty']} | [ ] | [ ] | [ ] | [ ] |")
        lines.append("")
    lines += ["## Reflection log", "", "| Date | Questions covered | What I learnt | What still confuses me |", "|---|---|---|---|", "| | | | |", ""]
    return "\n".join(lines)


def main():
    qs = flatten()
    problems = validate(qs)
    if problems:
        print("VALIDATION FAILED:")
        for p in problems:
            print(" -", p)
        sys.exit(1)
    nb = build_notebook(qs)
    nb_path = os.path.join(OUT, "02_sql_practice_250_questions.ipynb")
    with open(nb_path, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
        f.write("\n")
    tr_path = os.path.join(OUT, "04_sql_learning_tracker.md")
    with open(tr_path, "w", encoding="utf-8") as f:
        f.write(build_tracker(qs))
    print(f"OK: {len(qs)} questions validated; wrote {nb_path} and {tr_path}")
    print("Difficulty totals:", dict(Counter(q["difficulty"] for q in qs)))


if __name__ == "__main__":
    main()
