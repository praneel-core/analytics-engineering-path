# SQL for Analytics Engineering — Concept Reference (Databricks SQL / Spark SQL)

A companion to the 250-question lab. It explains the ideas you need **before and while** solving the exercises.
All examples use small *toy* tables (below) — never the lab's e-commerce tables — so reading this guide will not hand you
the answer to any practice question. Where an example looks similar to a lab task, the lab asks you to apply it to
different tables and richer business rules.

**Contents**
1. [Toy tables used in the examples](#0-toy-tables)
2. [SQL logical execution order](#1-sql-logical-execution-order)
3. [WHERE vs HAVING](#2-where-vs-having)
4. [INNER JOIN vs LEFT JOIN (and the other joins)](#3-joins)
5. [GROUP BY vs window functions](#4-group-by-vs-window-functions)
6. [ROW_NUMBER vs RANK vs DENSE_RANK](#5-row_number-vs-rank-vs-dense_rank)
7. [CTEs vs subqueries](#6-ctes-vs-subqueries)
8. [EXISTS vs IN](#7-exists-vs-in)
9. [UNION vs UNION ALL (and INTERSECT / EXCEPT)](#8-union-vs-union-all)
10. [NULL handling](#9-null-handling)
11. [Aggregate functions](#10-aggregate-functions)
12. [Window frame specifications](#11-window-frame-specifications)
13. [Date and timestamp operations](#12-date-and-timestamp-operations)
14. [String and regular-expression functions](#13-string-and-regex-functions)
15. [Deduplication strategies](#14-deduplication-strategies)
16. [Fact and dimension table queries](#15-fact-and-dimension-queries)
17. [Slowly Changing Dimensions](#16-slowly-changing-dimensions)
18. [Incremental data processing](#17-incremental-data-processing)
19. [Query optimisation concepts](#18-query-optimisation-concepts)
20. [How this maps to dbt](#19-how-this-maps-to-dbt)
21. [Gotcha checklist](#20-gotcha-checklist)

---

## 0. Toy tables

```
employees(emp_id, name, dept_id, salary, hire_date, manager_id)     -- manager_id NULL for the CEO
departments(dept_id, dept_name)                                      -- some departments have no employees
sales(sale_id, rep_id, sale_date, amount, region)                    -- rep_id -> employees.emp_id
```

| emp_id | name | dept_id | salary | manager_id |
|---|---|---|---|---|
| 1 | Asha | 10 | 150 | NULL |
| 2 | Bala | 10 | 120 | 1 |
| 3 | Chitra | 20 | 120 | 1 |
| 4 | Dev | 20 | 90 | 3 |
| 5 | Esha | NULL | 80 | 3 |

---

## 1. SQL logical execution order

**What it is.** You *write* `SELECT … FROM … WHERE …` but the engine *logically evaluates* in a different order:

| Step | Clause | What happens |
|---|---|---|
| 1 | `FROM` / `JOIN` | build the working row set |
| 2 | `WHERE` | filter rows (no aggregates, no window functions, usually no SELECT aliases) |
| 3 | `GROUP BY` | collapse rows into groups |
| 4 | `HAVING` | filter groups (aggregates allowed) |
| 5 | `SELECT` | compute expressions and aliases |
| 6 | window functions | evaluated on the rows that survive 1–4 (they live in `SELECT`/`ORDER BY`) |
| 7 | `DISTINCT` | remove duplicate output rows |
| 8 | `ORDER BY` | sort (may use SELECT aliases) |
| 9 | `LIMIT` | cut the result |

(Databricks also has `QUALIFY`, which filters on window functions after step 6 — handy but not portable to every engine.)

**When it matters.** Whenever you wonder *"why can't I use this alias / window / aggregate here?"*.

**Common mistakes**
* Using a SELECT alias in `WHERE` (not allowed; Databricks lets later expressions in the *same SELECT list* reuse earlier aliases, but `WHERE` runs before `SELECT`).
* Putting an aggregate in `WHERE` (use `HAVING`).
* Filtering a window function in `WHERE` (it does not exist yet — wrap in a subquery/CTE, or use `QUALIFY`).
* Expecting `LIMIT` without `ORDER BY` to be repeatable.

```sql
-- Wrong: the alias n does not exist yet when WHERE runs
SELECT dept_id, COUNT(*) AS n FROM employees WHERE n > 1 GROUP BY dept_id;

-- Right
SELECT dept_id, COUNT(*) AS n FROM employees GROUP BY dept_id HAVING COUNT(*) > 1;
```

---

## 2. WHERE vs HAVING

**What they do.** `WHERE` filters **rows before** grouping. `HAVING` filters **groups after** aggregation.

**When to use**
* Use `WHERE` for any condition on raw columns — it shrinks the data early (faster).
* Use `HAVING` only for conditions on aggregate results (`COUNT(*) > 1`, `SUM(x) > 100`).

**Common mistakes**
* Putting a row-level condition in `HAVING` (works but forces grouping of unnecessary rows).
* Using `WHERE` for an aggregate (error).
* Forgetting that `HAVING` can reference grouped columns, aggregates — not window functions.

```sql
SELECT region, SUM(amount) AS total
FROM sales
WHERE sale_date >= DATE'2025-01-01'     -- row filter: before grouping
GROUP BY region
HAVING SUM(amount) > 10000;             -- group filter: after grouping
```

---

## 3. Joins

| Join | Returns | Typical use |
|---|---|---|
| `INNER JOIN` | only rows with a match on both sides | fact → dimension lookups where the dimension must exist |
| `LEFT JOIN` | every left row; right columns are NULL if no match | keep all facts even if a dimension is missing; find unmatched rows |
| `RIGHT JOIN` | every right row (mirror of LEFT — most people just swap the tables) | rarely needed |
| `FULL OUTER JOIN` | all rows from both sides | reconciling two sources |
| `CROSS JOIN` | every combination (Cartesian product) | building a grid / date spine × entity list |
| self join | a table joined to itself with aliases | hierarchies (employee → manager), pairs, previous/next row |
| semi join (`EXISTS` / `LEFT SEMI JOIN`) | left rows that have a match, no right columns | filter by existence without duplicating rows |
| anti join (`NOT EXISTS` / `LEFT ANTI JOIN`) | left rows with **no** match | find missing relationships |

**INNER vs LEFT in one sentence.** `INNER` silently drops unmatched rows; `LEFT` keeps them with NULLs — so a `LEFT JOIN`
followed by a `WHERE` on a right-side column can *accidentally turn it into an inner join*.

```sql
-- Employees and their department name; employee 5 has no department
SELECT e.name, d.dept_name
FROM employees e
LEFT JOIN departments d ON d.dept_id = e.dept_id;       -- Esha is kept, dept_name is NULL

-- Departments with no employees (anti join via LEFT JOIN)
SELECT d.dept_name
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id
WHERE e.emp_id IS NULL;

-- Filter on the right table INSIDE the ON clause to keep the left-join behaviour
SELECT d.dept_name, e.name
FROM departments d
LEFT JOIN employees e ON e.dept_id = d.dept_id AND e.salary > 100;
```

**Cardinality and fan-out (the #1 source of wrong numbers).**
Joining a parent to a child in a one-to-many relationship repeats the parent row once per child. Summing a parent column afterwards multiplies it.

```sql
-- If departments had a budget column, this would count each budget once per employee (wrong):
SELECT d.dept_name, SUM(d.budget) FROM departments d JOIN employees e USING (dept_id) GROUP BY d.dept_name;

-- Fix: aggregate the child first, then join (one row per department)
WITH emp AS (SELECT dept_id, COUNT(*) AS headcount FROM employees GROUP BY dept_id)
SELECT d.dept_name, d.budget, COALESCE(emp.headcount, 0) AS headcount
FROM departments d LEFT JOIN emp USING (dept_id);
```

**Joining two child tables to the same parent** (employees → sales *and* employees → timesheets) multiplies rows
(sales × timesheets per employee). Aggregate each child to the parent grain first.

**Common mistakes**
* Joining on a non-unique key and not realising it (`COUNT(*)` vs `COUNT(DISTINCT key)` check).
* `COUNT(*)` after a `LEFT JOIN` counts the NULL-extended row as 1 — use `COUNT(right.key)`.
* Comparing NULL keys: `NULL = NULL` is not true, so NULL keys never join.
* Forgetting a join condition ⇒ accidental Cartesian product.

---

## 4. GROUP BY vs window functions

| | `GROUP BY` | window function |
|---|---|---|
| Output rows | one per group (collapses detail) | one per input row (keeps detail) |
| Lets you also show row-level columns? | no | yes |
| Typical use | totals, counts, KPIs | running totals, ranks, % of total, previous/next row |

```sql
-- GROUP BY: one row per department
SELECT dept_id, AVG(salary) AS avg_salary FROM employees GROUP BY dept_id;

-- Window: every employee keeps their row AND sees the department average
SELECT name, dept_id, salary,
       AVG(salary) OVER (PARTITION BY dept_id) AS dept_avg,
       salary - AVG(salary) OVER (PARTITION BY dept_id) AS vs_avg
FROM employees;
```

**When to use which.** Need a smaller table → `GROUP BY`. Need to compare a row with its group or with neighbouring rows → window.
You can combine them: aggregate with `GROUP BY` in a CTE, then apply a window function on the aggregated result (e.g. rank departments by total salary).

**Common mistakes**
* Forgetting that the window runs *after* grouping when both are in one query: `SUM(SUM(x)) OVER (...)` is legal and means "window over the group sums".
* Missing `PARTITION BY` ⇒ the window spans the whole table.
* Missing `ORDER BY` in a window that needs it (running totals, `LAG`).

---

## 5. ROW_NUMBER vs RANK vs DENSE_RANK

Given salaries 150, 120, 120, 90 ordered descending:

| salary | `ROW_NUMBER` | `RANK` | `DENSE_RANK` |
|---|---|---|---|
| 150 | 1 | 1 | 1 |
| 120 | 2 | 2 | 2 |
| 120 | 3 | 2 | 2 |
| 90 | 4 | 4 | 3 |

* `ROW_NUMBER` — unique sequence, ties broken arbitrarily (**add a tie-breaker column** to make it deterministic).
* `RANK` — ties share a rank and the next rank is skipped (1, 2, 2, 4).
* `DENSE_RANK` — ties share a rank, no gaps (1, 2, 2, 3).

**When to use**
* Exactly one row per group (latest record, dedupe): `ROW_NUMBER`.
* "Top N including ties": `RANK` or `DENSE_RANK` with `<= N`.
* "Top N distinct values" (3rd highest salary): `DENSE_RANK`.

```sql
SELECT name, dept_id, salary,
       ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, emp_id) AS rn,
       RANK()       OVER (PARTITION BY dept_id ORDER BY salary DESC)          AS rnk,
       DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC)          AS drnk
FROM employees;
```

**Common mistakes:** using `RANK` when you need exactly one row per key; filtering `rn = 1` in the same `SELECT` where it is computed (use a CTE/subquery or `QUALIFY`); non-deterministic `ORDER BY` ties.

Related: `NTILE(n)` (equal-size buckets), `PERCENT_RANK()`, `CUME_DIST()`.

---

## 6. CTEs vs subqueries

| | CTE (`WITH name AS (...)`) | subquery |
|---|---|---|
| Readability | names each step; read top-to-bottom | nested; harder to follow |
| Reuse | can be referenced several times | must be repeated |
| Recursion | supports `WITH RECURSIVE` on newer runtimes | no |
| Performance | usually inlined by the optimiser, same plan as subquery (not materialised by default) | same |

**Subquery kinds**
* **Scalar** — returns one value: `WHERE salary > (SELECT AVG(salary) FROM employees)`.
* **Derived table** — in `FROM`: `FROM (SELECT ... GROUP BY ...) t`.
* **Correlated** — refers to the outer row, conceptually re-evaluated per row:
  `WHERE salary > (SELECT AVG(salary) FROM employees x WHERE x.dept_id = e.dept_id)`. Often rewritable as a join to a pre-aggregated CTE or a window function.
* `IN` / `EXISTS` subqueries (next section).

```sql
WITH dept_totals AS (
  SELECT dept_id, SUM(salary) AS payroll FROM employees GROUP BY dept_id
), ranked AS (
  SELECT *, RANK() OVER (ORDER BY payroll DESC) AS r FROM dept_totals
)
SELECT * FROM ranked WHERE r <= 2;
```

**Why analytics engineers love CTEs:** every dbt model is typically "import CTEs → logical CTEs → final select".
**Common mistakes:** assuming a CTE is cached (it is not, unless you materialise it); scalar subqueries returning more than one row (error); correlated subqueries on huge tables when a join would do.

**Recursive CTE sketch** (employee hierarchy; requires a runtime that supports `WITH RECURSIVE`; otherwise use repeated self-joins for fixed depth):
```sql
WITH RECURSIVE org AS (
  SELECT emp_id, name, manager_id, 1 AS depth FROM employees WHERE manager_id IS NULL
  UNION ALL
  SELECT e.emp_id, e.name, e.manager_id, o.depth + 1
  FROM employees e JOIN org o ON e.manager_id = o.emp_id
)
SELECT * FROM org;
```

---

## 7. EXISTS vs IN

| | `IN (subquery)` | `EXISTS (subquery)` |
|---|---|---|
| Meaning | value is in the list returned | at least one matching row exists |
| NULL behaviour | `NOT IN` returns **no rows** if the list contains a NULL | `NOT EXISTS` is NULL-safe |
| Multi-column match | awkward | natural (correlate on several columns) |
| Typical use | small, clean lists | existence checks, anti joins |

```sql
-- Departments that have at least one employee earning above 100
SELECT d.dept_name FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.dept_id AND e.salary > 100);

-- NOT IN trap: if any manager_id is NULL, the whole predicate is never true
SELECT name FROM employees WHERE emp_id NOT IN (SELECT manager_id FROM employees);          -- returns nothing!
SELECT name FROM employees e WHERE NOT EXISTS (SELECT 1 FROM employees m WHERE m.manager_id = e.emp_id);  -- correct
```

**Rule of thumb:** use `EXISTS`/`NOT EXISTS` for existence and anti-joins; use `IN` for short literal lists. The optimiser usually plans both as semi/anti joins.

---

## 8. UNION vs UNION ALL

| | `UNION` | `UNION ALL` |
|---|---|---|
| Duplicates | removed (a distinct step — shuffle + sort/hash) | kept |
| Speed | slower | faster |
| Use | when you need set semantics | stacking same-shaped data (almost always in pipelines) |

* Column count and compatible types must match; names come from the first query.
* `INTERSECT` = rows in both; `EXCEPT` (or `MINUS`) = rows in the first but not the second.
* Add a literal `source` column to track where each row came from.

```sql
SELECT emp_id, 'current' AS src FROM employees
UNION ALL
SELECT emp_id, 'alumni'  AS src FROM former_employees;
```
**Mistake:** using `UNION` "just in case" and silently paying for a global de-duplication, or hiding genuine duplicates you should have investigated.

---

## 9. NULL handling

NULL means *unknown*, not zero or empty. SQL uses three-valued logic (TRUE / FALSE / UNKNOWN).

| Situation | Behaviour |
|---|---|
| `NULL = NULL`, `NULL <> 5` | UNKNOWN (row is filtered out) — use `IS NULL`, `IS NOT NULL`, or null-safe equality `<=>` |
| `WHERE x <> 'A'` | excludes rows where x is NULL |
| `NULL + 1`, `concat(a, NULL)` (in `+`/arith) | NULL (`concat_ws` skips NULLs) |
| `COUNT(col)` | skips NULLs; `COUNT(*)` counts rows |
| `SUM`, `AVG`, `MIN`, `MAX` | ignore NULLs (all-NULL group ⇒ NULL) |
| `GROUP BY` / `DISTINCT` | treat all NULLs as one group |
| `ORDER BY` | NULLs first for ASC and last for DESC by default in Spark; use `NULLS FIRST/LAST` |
| `NOT IN` with NULL in the list | never true |

Useful functions:
```sql
COALESCE(a, b, 0)         -- first non-NULL
NULLIF(x, 0)              -- NULL when x = 0 -> protects divisions: a / NULLIF(b, 0)
IFNULL(a, b) / NVL(a, b)  -- two-argument COALESCE
x IS DISTINCT FROM y      -- NULL-safe "not equal"
CASE WHEN x IS NULL THEN 'unknown' ELSE x END
```
**Common mistakes:** `AVG` after replacing NULL with 0 (changes the meaning — is "missing" really zero?); relying on `<> 'X'` to include NULL rows; dividing by zero (ANSI mode raises an error — use `NULLIF` or `try_divide`).

---

## 10. Aggregate functions

| Function | Notes |
|---|---|
| `COUNT(*)` / `COUNT(col)` / `COUNT(DISTINCT col)` | rows / non-NULL / unique non-NULL |
| `SUM`, `AVG`, `MIN`, `MAX` | ignore NULLs; `AVG` of an integer column returns a decimal/double |
| `COUNT_IF(cond)`, `SUM(CASE WHEN cond THEN 1 ELSE 0 END)` | conditional counting |
| `MAX_BY(x, y)`, `MIN_BY(x, y)` | value of x at the row where y is max/min |
| `ANY_VALUE`, `FIRST`, `LAST` | pick an arbitrary / first / last value (order not guaranteed without care) |
| `COLLECT_LIST`, `COLLECT_SET`, `ARRAY_AGG`, `STRING_AGG`/`LISTAGG` equivalents | gather values |
| `PERCENTILE`, `PERCENTILE_APPROX`, `MEDIAN` | quantiles (approx is much cheaper on big data) |
| `STDDEV`, `VARIANCE`, `CORR` | statistics |
| `APPROX_COUNT_DISTINCT` | HyperLogLog distinct count |

**Conditional aggregation pivot**
```sql
SELECT region,
       SUM(CASE WHEN YEAR(sale_date) = 2024 THEN amount END) AS amt_2024,
       SUM(CASE WHEN YEAR(sale_date) = 2025 THEN amount END) AS amt_2025
FROM sales GROUP BY region;
```
**Mistakes:** mixing aggregated and non-aggregated columns without `GROUP BY`; `COUNT(DISTINCT)` on a fan-out join; averaging averages (average of per-group averages ≠ overall average — carry sums and counts); integer division when computing ratios (multiply by `1.0` or cast).

---

## 11. Window frame specifications

A window function has three parts: `PARTITION BY` (which rows), `ORDER BY` (in what order) and the **frame** (which neighbouring rows are visible).

```sql
fn(...) OVER (
  PARTITION BY dept_id
  ORDER BY hire_date
  ROWS BETWEEN 2 PRECEDING AND CURRENT ROW      -- the frame
)
```

| Frame | Meaning |
|---|---|
| `ROWS BETWEEN n PRECEDING AND CURRENT ROW` | physical rows — "this row and the n before it" |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | running total/cumulative |
| `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` | the whole partition |
| `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` | centred 3-row window |
| `RANGE BETWEEN … ` | **logical** range based on the ORDER BY *value* (ties are included together; supports numeric offsets or `INTERVAL` on date/timestamp ordering) |

**Defaults to remember**
* With `ORDER BY` and no frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (so **ties are summed together** in running totals).
* Without `ORDER BY`: the frame is the entire partition.
* `FIRST_VALUE` works as expected; `LAST_VALUE` with the default frame returns the **current row** — specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
* `ROW_NUMBER`, `RANK`, `LAG`, `LEAD` ignore frames.

```sql
SELECT sale_date, amount,
       SUM(amount) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
       AVG(amount) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)         AS moving_avg_7_rows,
       SUM(amount) OVER (ORDER BY sale_date RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW) AS trailing_7_days
FROM sales_daily;
```
**ROWS vs RANGE:** if some dates are missing, `ROWS 6 PRECEDING` covers "7 rows" (possibly many weeks); a day-based `RANGE` covers exactly 7 calendar days. For continuous series, build a **date spine** and `LEFT JOIN` so missing days become zero rows.

**Mistakes:** forgetting `ORDER BY` inside the window; assuming `RANGE` ≡ `ROWS`; `LAG` across partitions because `PARTITION BY` was omitted; filtering the window result in the same query level.

---

## 12. Date and timestamp operations

Types: `DATE` (no time), `TIMESTAMP` (instant, displayed in the session time zone), `TIMESTAMP_NTZ` (no time zone), `INTERVAL`.

| Task | Functions |
|---|---|
| Current | `current_date()`, `current_timestamp()` (avoid in reproducible exercises — use a fixed snapshot date) |
| Literals / conversion | `DATE'2025-12-31'`, `TIMESTAMP'2025-12-31 10:00:00'`, `to_date(ts)`, `CAST(ts AS DATE)`, `to_timestamp(str, fmt)`, `to_date(str, 'dd/MM/yyyy')` |
| Parts | `year()`, `quarter()`, `month()`, `day()`/`dayofmonth()`, `hour()`, `dayofweek()` (1 = Sunday), `weekofyear()`, `date_format(ts, 'EEEE')` |
| Truncate | `date_trunc('MONTH', ts)` (returns timestamp), `trunc(date, 'MM')` (returns date); units: YEAR, QUARTER, MONTH, WEEK (Monday start), DAY, HOUR… |
| Arithmetic | `date_add(d, n)`, `date_sub(d, n)`, `add_months(d, n)`, `d + INTERVAL 7 DAYS`, `dateadd(DAY, n, ts)` |
| Differences | `datediff(end, start)` (days), `months_between(end, start)`, `timestampdiff(HOUR, start, end)`, `unix_timestamp(end) - unix_timestamp(start)` (seconds) |
| Ends | `last_day(d)`, `next_day(d, 'Mon')` |
| Sequences | `sequence(DATE'2025-01-01', DATE'2025-01-31', INTERVAL 1 DAY)` + `explode()` → date spine |

**The timestamp BETWEEN trap.** `ts BETWEEN '2025-12-01' AND '2025-12-31'` stops at *midnight* of the 31st, so almost all of the 31st is lost.
Prefer a half-open range:
```sql
WHERE ts >= DATE'2025-12-01' AND ts < DATE'2026-01-01'
```
**Business days:** `dim_date`-style calendar table with `is_weekend`/`is_holiday` is more reliable than formulas.
**Period-over-period:** `LAG(value, 1)` on a monthly aggregate, or self-join on `add_months(month, -12)` for same-period-last-year.
**Mistakes:** grouping a timestamp directly (each second a group); mixing `date_trunc` (timestamp) with date literals; `dayofweek` numbering assumptions; time-zone surprises when casting timestamps to dates; using `current_date()` in a model that must be re-runnable.

---

## 13. String and regex functions

| Task | Functions |
|---|---|
| Case / whitespace | `lower`, `upper`, `initcap`, `trim`, `ltrim`, `rtrim`, `btrim(str, chars)` |
| Pieces | `substr(s, pos, len)`, `left`, `right`, `split(s, regex)`, `split_part(s, delim, n)`, `element_at(array, i)` |
| Search | `s LIKE 'a%'`, `ILIKE`, `instr`, `locate`, `contains`, `startswith`, `endswith` |
| Combine | `concat`, `concat_ws(sep, ...)` (skips NULLs), `lpad`, `rpad`, `repeat` |
| Replace | `replace(s, a, b)`, `translate`, `regexp_replace(s, pattern, repl)` |
| Regex | `s RLIKE 'pattern'`, `regexp_like`, `regexp_extract(s, pattern, groupIdx)`, `regexp_extract_all`, `regexp_count` |
| Normalise | `lower(trim(x))`, `regexp_replace(x, '[^0-9]', '')` (digits only), `md5`, `sha2` |

Escaping: in Spark SQL string literals a backslash is itself an escape character (unless you use `r'...'` raw literals), so a literal dot in a regex is `'\\.'`.
```sql
SELECT email,
       email RLIKE '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$'  AS looks_valid,
       regexp_extract(email, '@(.+)$', 1)                        AS domain
FROM people;
```
**Standardisation recipe:** `trim` → `lower` (or `initcap`) → map aliases with `CASE` or a small joined mapping table (a dbt *seed*). Always check distinct raw values first.
**Mistakes:** `=` comparisons on un-normalised text; forgetting that `LIKE '_'` is a single-character wildcard; unescaped `%`/`_`; `initcap` on acronyms (`UPI` → `Upi`).

---

## 14. Deduplication strategies

First **define the business key** and **which copy wins** (earliest? latest? most complete?). Then pick:

| Strategy | Pattern | Notes |
|---|---|---|
| Exact duplicates | `SELECT DISTINCT *` | only for identical rows; hides the problem |
| Keep one per key | `ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC, tie_breaker) = 1` | the standard, deterministic approach |
| Latest via aggregate | `MAX_BY(col, updated_at)` with `GROUP BY key` | concise for a few columns |
| Aggregate to a golden record | `MIN(created_at)`, `MAX(updated_at)`, `ANY_VALUE`… | when you merge attributes |
| Delta `MERGE` | `MERGE INTO t USING (deduped source) s ON t.key = s.key` | **de-duplicate the source first** — duplicates in the source break a merge |
| Detect first | `GROUP BY key HAVING COUNT(*) > 1`; `COUNT(*) OVER (PARTITION BY key)` | always quantify before deleting |

```sql
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY lower(trim(email)) ORDER BY created_at, id) AS rn
  FROM people_raw
)
SELECT * FROM ranked WHERE rn = 1;          -- or QUALIFY rn = 1 in Databricks
```
**Mistakes:** dedup key not normalised (case/space); no deterministic tie-break; deleting before understanding *why* duplicates exist (replays? double submits? genuinely different people?); NULL keys — decide how to treat them (usually keep them all).

---

## 15. Fact and dimension queries

| Concept | Meaning |
|---|---|
| **Grain** | what one row of a table represents ("one row per order line") — decide it first and test it |
| **Fact table** | events/measurements at a stated grain: foreign keys + numeric measures (additive, semi-additive, non-additive) |
| **Dimension table** | descriptive attributes (who/what/where/when) with one row per member (or per version for SCD2) |
| **Surrogate key** | warehouse-generated key independent of source ids (`row_number()`, `md5(concat_ws('||', ...))`, `xxhash64(...)`, or identity columns) |
| **Natural / business key** | the source system's identifier |
| **Conformed dimension** | one shared dimension used by many facts (date, customer, product) |
| **Degenerate dimension** | an identifier stored on the fact (order number) |
| **Star schema** | central fact table joined to flat dimensions; **snowflake** normalises dimensions further |

**Querying a star:** start from the fact, `JOIN` many-to-one to each dimension, `GROUP BY` dimension attributes, `SUM` measures.
```sql
SELECT d.year, p.category, SUM(f.amount) AS revenue
FROM fact_sales f
JOIN dim_date d    ON d.date_key = f.date_key
JOIN dim_product p ON p.product_key = f.product_key
GROUP BY d.year, p.category;
```
**Grain tests (run them!):**
```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT order_id, line_no) AS distinct_keys FROM fact_order_lines;   -- must be equal
```
**Rules of thumb:** facts join to dimensions many-to-one (never the reverse); keep measures at the finest grain; make a *unknown member* row (key −1) in each dimension so late-arriving or missing dimension rows do not drop facts; keep aggregates (marts) separate from atomic facts; do not add a measure from a coarser grain to a finer one without allocation (fan-out).
**Mistakes:** mixed grains in one fact; summing a snapshot measure (balance, stock) across time; joining facts to each other directly instead of through conformed dimensions or pre-aggregation.

---

## 16. Slowly Changing Dimensions

Attributes of a dimension change over time (a customer moves city). How you record that is the SCD *type*.

| Type | Behaviour | History kept? | Typical SQL |
|---|---|---|---|
| **0** | never changes after load | – | insert only |
| **1** | overwrite with the latest value | no | `MERGE … WHEN MATCHED THEN UPDATE SET …` |
| **2** | add a new row per change, with validity dates | yes | `valid_from`, `valid_to`, `is_current` (and a new surrogate key) |
| **3** | keep "previous value" in an extra column | one step back | `prev_city` column |
| **6** | hybrid 1+2+3 | yes | current-value column on every historical row |

**Type 2 anatomy**
```
customer_key | customer_id | city   | valid_from | valid_to   | is_current
1            | 100         | Pune   | 2023-01-01 | 2024-05-01 | false
2            | 100         | Mumbai | 2024-05-01 | 9999-12-31 | true
```
Build the validity range from a change log with `LEAD`:
```sql
SELECT id, city, change_ts AS valid_from,
       COALESCE(LEAD(change_ts) OVER (PARTITION BY id ORDER BY change_ts), TIMESTAMP'9999-12-31') AS valid_to,
       LEAD(change_ts) OVER (PARTITION BY id ORDER BY change_ts) IS NULL AS is_current
FROM change_log;
```
**Point-in-time join** (attribute *as of the event*): `JOIN dim d ON d.id = f.id AND f.event_ts >= d.valid_from AND f.event_ts < d.valid_to` (half-open ranges avoid double matches).
**When to choose:** Type 1 for corrections and attributes where history is meaningless; Type 2 when reports must reflect "what was true then" (region of a sales rep, customer segment at purchase).
**In dbt:** `snapshots` implement Type 2 (`check` or `timestamp` strategy) — they write `dbt_valid_from`, `dbt_valid_to`.
**Mistakes:** overlapping or gapped validity ranges; using a closed upper bound on both ends; late-arriving changes processed in arrival order instead of event order; forgetting to dedupe the change feed first; joining a fact only on the business key (matches all versions ⇒ fan-out).

---

## 17. Incremental data processing

**Idea.** Don't rebuild everything — process only new/changed source rows, then upsert into the target.

| Piece | Explanation |
|---|---|
| **Watermark** | high-water mark of data already processed (`MAX(updated_at)` in the target) |
| **Filter** | `WHERE src.updated_at > (SELECT MAX(updated_at) FROM target)` (often with a small overlap/lookback to absorb late data) |
| **Unique key** | what identifies a row for upserts (`order_id`) — must be unique in the *incoming* set after dedupe |
| **Merge/upsert** | insert new keys, update changed keys |
| **Late-arriving facts** | rows whose business date is old but which arrive now — filtering by *business* date (`order_date`) misses them; filter by *ingestion/update* time instead |
| **Late-arriving dimensions** | fact references a dimension member that does not exist yet — keep the fact with an "unknown" key and fix later |
| **Idempotency** | re-running the same batch gives the same result (dedupe + deterministic merge) |
| **Change detection** | compare a hash of the tracked columns: `md5(concat_ws('|', a, b, c))` |

```sql
-- Classify incoming rows (new / changed / unchanged)
SELECT s.*,
       CASE WHEN t.id IS NULL THEN 'new'
            WHEN md5(concat_ws('|', s.a, s.b)) <> md5(concat_ws('|', t.a, t.b)) THEN 'changed'
            ELSE 'unchanged' END AS change_type
FROM staged s LEFT JOIN target t ON t.id = s.id;

-- Delta Lake upsert (dedupe `staged` first!)
MERGE INTO target t
USING (SELECT * FROM staged_deduped) s ON t.id = s.id
WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```
**dbt mapping:** `materialized='incremental'`, `unique_key`, `incremental_strategy='merge'`, `{% if is_incremental() %} where updated_at > (select max(updated_at) from {{ this }}) {% endif %}`; add `on_schema_change` and use a lookback window for late data.
**Mistakes:** non-unique keys in the batch (`MERGE` errors on multiple matches); using `>=` vs `>` without thinking about re-processing; watermark on a column that is not monotonically increasing; deleting rows is invisible to an incremental model (needs soft-delete flags or periodic full refresh).

---

## 18. Query optimisation concepts

Databricks/Delta specifics are marked ⚡. The goal is *correct first, then fast*.

1. **Read less data.** Select only needed columns (never `SELECT *` in production models); filter early; use partition/cluster columns in filters. ⚡ Delta stores column statistics and uses **data skipping** and **file pruning**.
2. **Filter before you join/aggregate.** Push `WHERE` conditions onto the smaller side; pre-aggregate large facts before joining to other facts.
3. **Understand join strategies.** *Broadcast hash join* (small table copied to every node) is best for fact × small dimension; *shuffle/sort-merge join* for two large tables. ⚡ Hints: `/*+ BROADCAST(dim) */`. Adaptive Query Execution (AQE) usually chooses well.
4. **Avoid accidental Cartesian products and fan-out** — check row counts after every join.
5. **Prefer `UNION ALL` over `UNION`, `EXISTS` over `IN` for big lists, window functions over self-joins where possible.**
6. **Don't wrap filter columns in functions** (`WHERE YEAR(ts) = 2025` can prevent pruning; prefer range predicates on the raw column).
7. **Approximate when exactness is not needed:** `approx_count_distinct`, `percentile_approx`.
8. **Be careful with `DISTINCT`, `ORDER BY` and `COUNT(DISTINCT)` on huge sets** — they shuffle. Use `LIMIT` with `ORDER BY` for top-N.
9. **Skew:** one hot key (e.g. NULL or a default customer) overloads one task. ⚡ AQE skew-join handling helps; otherwise salt or filter the hot key.
10. **Small-file problem & layout.** ⚡ `OPTIMIZE table` compacts files; liquid clustering / `ZORDER BY` colocates related data for pruning; partition only very large tables by low-cardinality columns (e.g. date), not by high-cardinality ids.
11. **Cache sparingly.** ⚡ Serverless/Photon cache automatically; manual `CACHE TABLE` rarely needed.
12. **Read the plan.** `EXPLAIN FORMATTED SELECT …` — look for `BroadcastHashJoin` vs `SortMergeJoin`, `PartitionFilters`, `PushedFilters`, big `Exchange` (shuffle) nodes. In the Databricks UI use the *Query Profile*.
13. **Test with small samples first**, especially on Free Edition compute.

```sql
EXPLAIN FORMATTED
SELECT d.region, SUM(f.amount)
FROM big_fact f JOIN small_dim d ON f.dim_id = d.id
WHERE f.event_date >= DATE'2025-01-01'
GROUP BY d.region;
```

---

## 19. How this maps to dbt

| Concept in this guide | dbt feature |
|---|---|
| staging clean-up (trim, lower, cast, rename) | `stg_*` models (views), `source()` + `ref()` |
| CTE-per-step style | "import CTEs → logical CTEs → final" convention |
| dimensions / facts | `dim_*` / `fct_*` models in a marts layer (`materialized='table'`) |
| surrogate keys | `dbt_utils.generate_surrogate_key([...])` |
| SCD2 | `snapshots` |
| incremental processing | `materialized='incremental'` + `unique_key` + `is_incremental()` |
| mapping/lookups | `seeds` (CSV) |
| dedupe | `row_number()` pattern or `dbt_utils.deduplicate` |
| data tests | `unique`, `not_null`, `accepted_values`, `relationships` generic tests; singular tests are SQL that returns failing rows |
| reusable logic | Jinja macros |
| documentation | `description:` in YAML, `dbt docs generate` |

Every Level 9 exercise is the SQL core of one of these dbt objects.

---

## 20. Gotcha checklist

* Did I check the **grain** of each table I joined? (`COUNT(*)` vs `COUNT(DISTINCT key)`)
* Did the row count **change unexpectedly** after a join?
* Are my **filters on a LEFT-joined table in `ON`** (to keep unmatched rows) or in `WHERE`?
* Did I treat **NULL** deliberately (`IS NULL`, `COALESCE`, `NULLIF`, `NOT EXISTS`)?
* Am I comparing **normalised text** (trim/lower) and **clean dates** (half-open ranges)?
* Did I pick the right ranking function and add a **tie-breaker**?
* For windows: **partition**, **order**, **frame** — all three considered?
* Did I avoid `current_date()` where a fixed snapshot date is required?
* Is integer division hiding a decimal? (`100.0 * a / b`)
* Do my percentages use the **right denominator** (all rows vs filtered rows)?
* Can I explain, in a sentence, what one output row represents?
