# SQL Learning Tracker — 250 Questions

Copy this file or edit it in place. Replace `[ ]` with `[x]` as you go.

| Column | Meaning |
|---|---|
| Attempted | You wrote a query and ran it |
| Solved independently | Correct result without hints or looking anything up |
| Needed hints | You opened the hint cell |
| Revisit | Solve it again in a week (and always for anything you needed hints on) |

## Summary by difficulty

| Difficulty | Questions |
|---|---:|
| Beginner | 11 |
| Easy | 23 |
| Intermediate | 64 |
| Advanced | 103 |
| Expert | 49 |
| **Total** | **250** |

## Summary by level

| Level | Section | Questions | Beginner | Easy | Intermediate | Advanced | Expert | Solved (fill in) |
|---|---|---|---:|---:|---:|---:|---:|---|
| 1 | SQL Fundamentals | 1–25 (25) | 11 | 12 | 2 | 0 | 0 | ___ / 25 |
| 2 | Aggregations and Grouping | 26–50 (25) | 0 | 7 | 13 | 5 | 0 | ___ / 25 |
| 3 | Joins and Relationships | 51–80 (30) | 0 | 4 | 14 | 12 | 0 | ___ / 30 |
| 4 | Subqueries and CTEs | 81–105 (25) | 0 | 0 | 9 | 14 | 2 | ___ / 25 |
| 5 | Window Functions | 106–140 (35) | 0 | 0 | 10 | 18 | 7 | ___ / 35 |
| 6 | Date and Time Analytics | 141–160 (20) | 0 | 0 | 10 | 9 | 1 | ___ / 20 |
| 7 | Advanced Analytical SQL | 161–190 (30) | 0 | 0 | 0 | 16 | 14 | ___ / 30 |
| 8 | Data Cleaning and Data Quality | 191–210 (20) | 0 | 0 | 6 | 11 | 3 | ___ / 20 |
| 9 | Data Warehousing and Analytics Engineering SQL | 211–230 (20) | 0 | 0 | 0 | 14 | 6 | ___ / 20 |
| 10 | Advanced Business Scenarios and Interview Challenges | 231–250 (20) | 0 | 0 | 0 | 4 | 16 | ___ / 20 |

## Progress dashboard (update manually)

| Metric | Count |
|---|---:|
| Attempted | 0 / 250 |
| Solved independently | 0 / 250 |
| Needed hints | 0 |
| To revisit | 0 |

## Level 1: SQL Fundamentals (Questions 1–25)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 1 | List every warehouse | SELECT | Beginner | [ ] | [ ] | [ ] | [ ] |
| 2 | Customer names and cities | SELECT columns | Beginner | [ ] | [ ] | [ ] | [ ] |
| 3 | Peek at the orders table | LIMIT | Beginner | [ ] | [ ] | [ ] | [ ] |
| 4 | Find all customers from Mumbai | WHERE with equality | Beginner | [ ] | [ ] | [ ] | [ ] |
| 5 | Premium-priced products | WHERE with numeric comparison | Beginner | [ ] | [ ] | [ ] | [ ] |
| 6 | Cheapest products first | ORDER BY | Beginner | [ ] | [ ] | [ ] | [ ] |
| 7 | Top 10 most expensive products | ORDER BY DESC + LIMIT | Beginner | [ ] | [ ] | [ ] | [ ] |
| 8 | Which countries do customers live in? | DISTINCT | Beginner | [ ] | [ ] | [ ] | [ ] |
| 9 | Friendly column names | Column aliases | Beginner | [ ] | [ ] | [ ] | [ ] |
| 10 | Orders that are not delivered | Inequality operator | Beginner | [ ] | [ ] | [ ] | [ ] |
| 11 | Gold customers in Delhi | AND | Beginner | [ ] | [ ] | [ ] | [ ] |
| 12 | Orders paid by UPI or Wallet | IN / OR | Easy | [ ] | [ ] | [ ] | [ ] |
| 13 | Mid-priced products | BETWEEN | Easy | [ ] | [ ] | [ ] | [ ] |
| 14 | Customers who joined in 2024 | Date filtering | Easy | [ ] | [ ] | [ ] | [ ] |
| 15 | Products with 'Wireless' in the name | LIKE with wildcards | Easy | [ ] | [ ] | [ ] | [ ] |
| 16 | Gmail customers | LIKE (suffix match) | Easy | [ ] | [ ] | [ ] | [ ] |
| 17 | Customers with no email on file | IS NULL | Easy | [ ] | [ ] | [ ] | [ ] |
| 18 | Branded but discontinued products | IS NOT NULL + boolean | Easy | [ ] | [ ] | [ ] | [ ] |
| 19 | Margin per unit | Basic calculations | Easy | [ ] | [ ] | [ ] | [ ] |
| 20 | Margin percentage for smartphones | Calculations + ROUND | Easy | [ ] | [ ] | [ ] | [ ] |
| 21 | Orders placed on Christmas Day 2025 | Timestamp date filtering | Easy | [ ] | [ ] | [ ] | [ ] |
| 22 | Large festive-season orders still in flight | Combining AND / OR | Easy | [ ] | [ ] | [ ] | [ ] |
| 23 | Price bands for the catalogue | CASE WHEN | Easy | [ ] | [ ] | [ ] | [ ] |
| 24 | Customer age bands | CASE WHEN + date functions | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 25 | Effective discount percentage on lines | Calculations on order lines | Intermediate | [ ] | [ ] | [ ] | [ ] |

## Level 2: Aggregations and Grouping (Questions 26–50)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 26 | How many customers do we have? | COUNT(*) | Easy | [ ] | [ ] | [ ] | [ ] |
| 27 | Email coverage | COUNT(*) vs COUNT(column) | Easy | [ ] | [ ] | [ ] | [ ] |
| 28 | Total shipping collected | SUM | Easy | [ ] | [ ] | [ ] | [ ] |
| 29 | Average delivered order value | AVG with filter | Easy | [ ] | [ ] | [ ] | [ ] |
| 30 | Order date range | MIN / MAX | Easy | [ ] | [ ] | [ ] | [ ] |
| 31 | Orders per status | GROUP BY | Easy | [ ] | [ ] | [ ] | [ ] |
| 32 | Customers per country | GROUP BY + ORDER BY | Easy | [ ] | [ ] | [ ] | [ ] |
| 33 | Revenue by payment method for delivered orders | GROUP BY with SUM and COUNT | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 34 | Orders per year | GROUP BY on date part | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 35 | Cities with more than 500 customers | HAVING | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 36 | Customers by country and segment | Multi-column GROUP BY | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 37 | Distinct customers per order status | COUNT DISTINCT | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 38 | Best-selling products by units | SUM on order lines | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 39 | Order status mix by year | Conditional aggregation | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 40 | Monthly order counts for 2025 | Grouping by month | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 41 | Line value by quantity | AVG / COUNT by group | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 42 | Products sold in more than 100 orders | HAVING with COUNT DISTINCT | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 43 | First and last order per frequent customer | MIN / MAX per group | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 44 | Ten busiest days | GROUP BY date | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 45 | Payment attempt outcomes by method | Conditional counts | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 46 | Cancellation rate by payment method | Ratio via conditional aggregation | Advanced | [ ] | [ ] | [ ] | [ ] |
| 47 | Repeat cancellers | HAVING with conditional count | Advanced | [ ] | [ ] | [ ] | [ ] |
| 48 | Revenue by year and quarter | Multi-key date grouping | Advanced | [ ] | [ ] | [ ] | [ ] |
| 49 | Return reasons ranked | Aggregating returns | Advanced | [ ] | [ ] | [ ] | [ ] |
| 50 | Weekend versus weekday sales | Grouping by a derived flag | Advanced | [ ] | [ ] | [ ] | [ ] |

## Level 3: Joins and Relationships (Questions 51–80)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 51 | Orders with customer names | INNER JOIN | Easy | [ ] | [ ] | [ ] | [ ] |
| 52 | Products with their category | INNER JOIN | Easy | [ ] | [ ] | [ ] | [ ] |
| 53 | Stock held in the Mumbai warehouse | JOIN + WHERE | Easy | [ ] | [ ] | [ ] | [ ] |
| 54 | Which cities generate the most orders? | JOIN + GROUP BY | Easy | [ ] | [ ] | [ ] | [ ] |
| 55 | Order line detail view | Three-table join | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 56 | Revenue by leaf category | Fact-to-dimension join | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 57 | Customers who never ordered | LEFT JOIN anti-pattern | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 58 | Products that have never been sold | LEFT JOIN anti-pattern | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 59 | Orders per customer including zeros | LEFT JOIN with aggregation | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 60 | Categories with no products | RIGHT JOIN | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 61 | Orders and payments coverage check | FULL OUTER JOIN | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 62 | Planning grid: warehouse x top-level category | CROSS JOIN | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 63 | Category with its parent | SELF JOIN | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 64 | High-value orders in India | Join with multiple filters | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 65 | Sales by brand | Dimension attributes in aggregates | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 66 | How many rows does a join create? | Join cardinality | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 67 | Payments per order | One-to-many relationship | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 68 | Holiday versus normal days | Join to a date dimension | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 69 | Customer revenue without double counting | Preventing join fan-out | Advanced | [ ] | [ ] | [ ] | [ ] |
| 70 | Revenue by country and category | Multi-table fact/dimension join | Advanced | [ ] | [ ] | [ ] | [ ] |
| 71 | Order headers with no lines | Anti-join | Advanced | [ ] | [ ] | [ ] | [ ] |
| 72 | Delivered orders without a successful payment | Anti-join with a filter in ON | Advanced | [ ] | [ ] | [ ] | [ ] |
| 73 | Return rate by category | Joining a sparse fact table | Advanced | [ ] | [ ] | [ ] | [ ] |
| 74 | Available stock by product | Aggregating inventory across warehouses | Advanced | [ ] | [ ] | [ ] | [ ] |
| 75 | Customer records that share an email | SELF JOIN on a cleaned key | Advanced | [ ] | [ ] | [ ] | [ ] |
| 76 | Order, items and payments in one row | Joining several fact tables safely | Advanced | [ ] | [ ] | [ ] | [ ] |
| 77 | Browsed but never ordered | Anti-join across behaviour and sales | Advanced | [ ] | [ ] | [ ] | [ ] |
| 78 | Hours from order to shipment | Self join on an event log | Advanced | [ ] | [ ] | [ ] | [ ] |
| 79 | Biggest refund recipients | Chained joins through returns | Advanced | [ ] | [ ] | [ ] | [ ] |
| 80 | Orders that include products from more than one warehouse region | Join + HAVING on distinct counts | Advanced | [ ] | [ ] | [ ] | [ ] |

## Level 4: Subqueries and CTEs (Questions 81–105)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 81 | Products priced above the catalogue average | Scalar subquery | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 82 | Orders versus the overall average | Scalar subquery in SELECT | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 83 | Customers who paid with a wallet | IN subquery | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 84 | Products that were never returned | NOT IN subquery | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 85 | Customers with at least one cancelled order | EXISTS | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 86 | Active customers silent in 2025 | NOT EXISTS | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 87 | Average orders per customer | Derived table | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 88 | Top spenders with their city | Single CTE | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 89 | Customers living in a warehouse city | IN with a subquery on another table | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 90 | Orders above the customer's own average | Correlated subquery | Advanced | [ ] | [ ] | [ ] | [ ] |
| 91 | Latest order per customer via subquery | Correlated subquery with MAX | Advanced | [ ] | [ ] | [ ] | [ ] |
| 92 | Category share of revenue | Multiple CTEs | Advanced | [ ] | [ ] | [ ] | [ ] |
| 93 | Customers who out-spend the average customer | Nested subqueries | Advanced | [ ] | [ ] | [ ] | [ ] |
| 94 | Products sold in every year | CTE with HAVING | Advanced | [ ] | [ ] | [ ] | [ ] |
| 95 | Net revenue per order after refunds | Staged CTE logic | Advanced | [ ] | [ ] | [ ] | [ ] |
| 96 | Category tree with depth | Hierarchy / recursive-style | Advanced | [ ] | [ ] | [ ] | [ ] |
| 97 | Stocked products with no recent sales | NOT EXISTS with a date window | Advanced | [ ] | [ ] | [ ] | [ ] |
| 98 | Customers whose first order was in 2022 and who ordered in 2025 | Derived table with MIN/MAX | Advanced | [ ] | [ ] | [ ] | [ ] |
| 99 | All orders of the single biggest customer | Scalar subquery to find an id | Advanced | [ ] | [ ] | [ ] | [ ] |
| 100 | Monthly revenue against the best month | CTE and a scalar comparison | Advanced | [ ] | [ ] | [ ] | [ ] |
| 101 | Funnel by device | Multiple CTEs on event data | Advanced | [ ] | [ ] | [ ] | [ ] |
| 102 | Smartphone orders that also bought a case | EXISTS twice | Advanced | [ ] | [ ] | [ ] | [ ] |
| 103 | Customers who returned or cancelled | UNION vs UNION ALL | Advanced | [ ] | [ ] | [ ] | [ ] |
| 104 | Customer spend quartile cut-offs by stages | Staged CTEs with ranking | Expert | [ ] | [ ] | [ ] | [ ] |
| 105 | Products that beat their category average margin | Correlated logic vs CTE | Expert | [ ] | [ ] | [ ] | [ ] |

## Level 5: Window Functions (Questions 106–140)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 106 | Number each customer's orders | ROW_NUMBER | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 107 | Each customer's latest order | ROW_NUMBER for 'latest per group' | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 108 | Top 10 products by units, with ties | RANK | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 109 | Rank cities by customer count | DENSE_RANK | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 110 | RANK versus DENSE_RANK versus ROW_NUMBER | Ranking functions compared | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 111 | Each order's share of the customer's spend | SUM() OVER (PARTITION BY) | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 112 | Running revenue for December 2025 | Running total | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 113 | Cumulative order count per customer | COUNT() OVER | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 114 | Price versus category average | AVG() OVER | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 115 | Days between consecutive orders | LAG | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 116 | Next order and the wait for it | LEAD | Advanced | [ ] | [ ] | [ ] | [ ] |
| 117 | Compare every order with the customer's first order | FIRST_VALUE | Advanced | [ ] | [ ] | [ ] | [ ] |
| 118 | Last order status per customer | LAST_VALUE with an explicit frame | Advanced | [ ] | [ ] | [ ] | [ ] |
| 119 | 7-day moving average of daily orders | ROWS BETWEEN moving average | Advanced | [ ] | [ ] | [ ] | [ ] |
| 120 | 3-month moving average revenue | Moving average on months | Advanced | [ ] | [ ] | [ ] | [ ] |
| 121 | Average of the previous three orders | ROWS BETWEEN excluding current row | Advanced | [ ] | [ ] | [ ] | [ ] |
| 122 | Trailing 7-day revenue with RANGE | RANGE BETWEEN | Advanced | [ ] | [ ] | [ ] | [ ] |
| 123 | Duplicate customer records: which to keep? | Deduplication with ROW_NUMBER | Advanced | [ ] | [ ] | [ ] | [ ] |
| 124 | Duplicate gateway callbacks | Deduplication of payments | Advanced | [ ] | [ ] | [ ] | [ ] |
| 125 | Top 3 products per category by revenue | Top N per group | Advanced | [ ] | [ ] | [ ] | [ ] |
| 126 | Top 3 spenders in each city | Top N per group | Advanced | [ ] | [ ] | [ ] | [ ] |
| 127 | Customer spend quartiles | NTILE | Advanced | [ ] | [ ] | [ ] | [ ] |
| 128 | Price percentile within the category | PERCENT_RANK / CUME_DIST | Advanced | [ ] | [ ] | [ ] | [ ] |
| 129 | Order status transitions | LAG on an event log | Advanced | [ ] | [ ] | [ ] | [ ] |
| 130 | First and last purchase per customer | First/last transactions | Advanced | [ ] | [ ] | [ ] | [ ] |
| 131 | Month-over-month revenue growth | LAG on aggregated data | Advanced | [ ] | [ ] | [ ] | [ ] |
| 132 | Running share of revenue by city | Cumulative percentage | Advanced | [ ] | [ ] | [ ] | [ ] |
| 133 | Longest gap between orders | LAG + aggregation | Advanced | [ ] | [ ] | [ ] | [ ] |
| 134 | Reactivation after a long gap | LAG + CASE | Expert | [ ] | [ ] | [ ] | [ ] |
| 135 | Cumulative new customers by month | Window over a first-event CTE | Expert | [ ] | [ ] | [ ] | [ ] |
| 136 | What happens after add-to-cart? | LEAD on event data | Expert | [ ] | [ ] | [ ] | [ ] |
| 137 | Product rank movement month over month | RANK + LAG | Expert | [ ] | [ ] | [ ] | [ ] |
| 138 | Time between events in a session | LAG on timestamps | Expert | [ ] | [ ] | [ ] | [ ] |
| 139 | Orders higher than the previous order, twice in a row | LAG twice | Expert | [ ] | [ ] | [ ] | [ ] |
| 140 | Median order value per month | Percentiles by group | Expert | [ ] | [ ] | [ ] | [ ] |

## Level 6: Date and Time Analytics (Questions 141–160)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 141 | Break a timestamp into parts | Date part functions | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 142 | Orders by weekday | Day-of-week analysis | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 143 | Orders by hour of day | Time-of-day distribution | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 144 | Monthly revenue with date_trunc | Date truncation | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 145 | Days from order to last status update for delivered orders | Date differences | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 146 | Customer tenure at the snapshot date | Date arithmetic | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 147 | Orders in the final 30 days of data | Relative date windows | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 148 | Quarterly orders: 2024 against 2025 | Quarter analysis | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 149 | Weekly orders in November 2025 | Week truncation | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 150 | Signup cohorts older than 24 months | months_between / add_months | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 151 | Same month last year | Period comparison with a self-join | Advanced | [ ] | [ ] | [ ] | [ ] |
| 152 | Business days to refund | Business-day calculation with a calendar table | Advanced | [ ] | [ ] | [ ] | [ ] |
| 153 | Daily orders with zero-filled gaps | Date spine | Advanced | [ ] | [ ] | [ ] | [ ] |
| 154 | Fiscal year and quarter revenue | Fiscal calendar | Advanced | [ ] | [ ] | [ ] | [ ] |
| 155 | How late do orders arrive in the warehouse? | Timestamp differences | Advanced | [ ] | [ ] | [ ] | [ ] |
| 156 | Activation within 7 days of signup | Time-to-event by cohort | Advanced | [ ] | [ ] | [ ] | [ ] |
| 157 | Holiday effect on daily orders | Calendar attributes | Advanced | [ ] | [ ] | [ ] | [ ] |
| 158 | Time-of-day segments by payment method | Hour bucketing | Advanced | [ ] | [ ] | [ ] | [ ] |
| 159 | Month-end ordering rush | last_day / day-of-month logic | Advanced | [ ] | [ ] | [ ] | [ ] |
| 160 | Order-to-payment latency percentiles | Percentiles on durations | Expert | [ ] | [ ] | [ ] | [ ] |

## Level 7: Advanced Analytical SQL (Questions 161–190)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 161 | Customer lifetime value (simple) | Customer lifetime value | Advanced | [ ] | [ ] | [ ] | [ ] |
| 162 | Repeat purchase rate | Repeat purchase rate | Advanced | [ ] | [ ] | [ ] | [ ] |
| 163 | New versus returning customers per month | New vs returning | Advanced | [ ] | [ ] | [ ] | [ ] |
| 164 | Year-over-year revenue growth | Revenue growth | Advanced | [ ] | [ ] | [ ] | [ ] |
| 165 | Category contribution at the top level | Category contribution | Advanced | [ ] | [ ] | [ ] | [ ] |
| 166 | Product profitability scorecard | Product performance | Advanced | [ ] | [ ] | [ ] | [ ] |
| 167 | Top 5 products per top-level category by gross profit | Top N per group | Advanced | [ ] | [ ] | [ ] | [ ] |
| 168 | How many products make up 80% of revenue? | Pareto analysis | Advanced | [ ] | [ ] | [ ] | [ ] |
| 169 | Customer value segmentation | Revenue segmentation | Advanced | [ ] | [ ] | [ ] | [ ] |
| 170 | Payment method share of orders | Percentage of total | Advanced | [ ] | [ ] | [ ] | [ ] |
| 171 | Likely churned customers | Churn indicators | Advanced | [ ] | [ ] | [ ] | [ ] |
| 172 | CRM flag versus real behaviour | Identifying inactive customers | Advanced | [ ] | [ ] | [ ] | [ ] |
| 173 | Signup cohorts and first purchase | Cohort basics | Advanced | [ ] | [ ] | [ ] | [ ] |
| 174 | Cohort order matrix for the first six months | Cohort analysis | Advanced | [ ] | [ ] | [ ] | [ ] |
| 175 | Average order value by customer tenure | Tenure analysis | Advanced | [ ] | [ ] | [ ] | [ ] |
| 176 | Seasonality index | Seasonality | Advanced | [ ] | [ ] | [ ] | [ ] |
| 177 | Monthly retention by signup cohort | Cohort retention | Expert | [ ] | [ ] | [ ] | [ ] |
| 178 | Products often bought together | Market basket analysis | Expert | [ ] | [ ] | [ ] | [ ] |
| 179 | Category affinity | Market basket (categories) | Expert | [ ] | [ ] | [ ] | [ ] |
| 180 | Conversion funnel by device | Conversion analysis | Expert | [ ] | [ ] | [ ] | [ ] |
| 181 | Cart abandoners without an order that month | Activity vs sales | Expert | [ ] | [ ] | [ ] | [ ] |
| 182 | Top 3 products each month of 2025 | Top N per group | Expert | [ ] | [ ] | [ ] | [ ] |
| 183 | Top 5% of customers and their revenue share | Percentile analysis | Expert | [ ] | [ ] | [ ] | [ ] |
| 184 | CRM segment versus actual value tier | Segmentation cross-check | Expert | [ ] | [ ] | [ ] | [ ] |
| 185 | Discount depth versus returns | Discount effectiveness | Expert | [ ] | [ ] | [ ] | [ ] |
| 186 | Net revenue after refunds by month | Net revenue | Expert | [ ] | [ ] | [ ] | [ ] |
| 187 | Days from first to second order distribution | Repeat purchase interval | Expert | [ ] | [ ] | [ ] | [ ] |
| 188 | Reactivated customers per year | Win-back analysis | Expert | [ ] | [ ] | [ ] | [ ] |
| 189 | Revenue concentration by customer decile | Ranking and percentile analysis | Expert | [ ] | [ ] | [ ] | [ ] |
| 190 | Which cities drove 2025 growth? | Contribution analysis | Expert | [ ] | [ ] | [ ] | [ ] |

## Level 8: Data Cleaning and Data Quality (Questions 191–210)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 191 | Missing and malformed emails | NULL handling, pattern checks | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 192 | Standardise gender values | CASE WHEN + string functions | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 193 | Trim and case-fix customer names | TRIM, INITCAP | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 194 | Standardise city names | String standardisation + mapping | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 195 | Replace NULL brand and discount | COALESCE | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 196 | Safe division with NULLIF | NULLIF | Intermediate | [ ] | [ ] | [ ] | [ ] |
| 197 | People registered more than once | Duplicate identification | Advanced | [ ] | [ ] | [ ] | [ ] |
| 198 | Golden customer records | Deduplication | Advanced | [ ] | [ ] | [ ] | [ ] |
| 199 | Duplicate order lines | Duplicate detection + impact | Advanced | [ ] | [ ] | [ ] | [ ] |
| 200 | Email format validation | Regular expressions | Advanced | [ ] | [ ] | [ ] | [ ] |
| 201 | Extract the email domain | regexp_extract | Advanced | [ ] | [ ] | [ ] | [ ] |
| 202 | Implausible dates of birth | Invalid value detection | Advanced | [ ] | [ ] | [ ] | [ ] |
| 203 | Orders missing items, payments or products | Missing relationships | Advanced | [ ] | [ ] | [ ] | [ ] |
| 204 | Order header versus line mismatch | Consistency checks | Advanced | [ ] | [ ] | [ ] | [ ] |
| 205 | Short and over payments | Reconciliation checks | Advanced | [ ] | [ ] | [ ] | [ ] |
| 206 | Return quantity and refund anomalies | Business rule validation | Advanced | [ ] | [ ] | [ ] | [ ] |
| 207 | Sales outside the product's life | Temporal consistency | Advanced | [ ] | [ ] | [ ] | [ ] |
| 208 | Repeated failed payments in a day | Suspicious pattern detection | Expert | [ ] | [ ] | [ ] | [ ] |
| 209 | Unusual prices and quantities | Outlier detection | Expert | [ ] | [ ] | [ ] | [ ] |
| 210 | Inventory data integrity report | Multi-rule data quality query | Expert | [ ] | [ ] | [ ] | [ ] |

## Level 9: Data Warehousing and Analytics Engineering SQL (Questions 211–230)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 211 | Is the grain really one row per order line? | Identifying table grain | Advanced | [ ] | [ ] | [ ] | [ ] |
| 212 | stg_customers: a cleaned staging model | Staging / standardisation | Advanced | [ ] | [ ] | [ ] | [ ] |
| 213 | stg_payments with duplicate flag | Staging + deduplication flag | Advanced | [ ] | [ ] | [ ] | [ ] |
| 214 | dim_product with the full category path | Dimension construction | Advanced | [ ] | [ ] | [ ] | [ ] |
| 215 | Generating surrogate keys | Surrogate keys | Advanced | [ ] | [ ] | [ ] | [ ] |
| 216 | fact_order_items with dimension keys | Fact table construction | Advanced | [ ] | [ ] | [ ] | [ ] |
| 217 | fact_orders without fan-out | Fact table at order grain | Advanced | [ ] | [ ] | [ ] | [ ] |
| 218 | SCD Type 1: what would be overwritten? | Slowly changing dimensions (Type 1) | Advanced | [ ] | [ ] | [ ] | [ ] |
| 219 | SCD Type 2: build the history table | Slowly changing dimensions (Type 2) | Advanced | [ ] | [ ] | [ ] | [ ] |
| 220 | Point-in-time join to a Type 2 dimension | SCD2 joins | Advanced | [ ] | [ ] | [ ] | [ ] |
| 221 | Late-arriving change records | Handling late-arriving data | Advanced | [ ] | [ ] | [ ] | [ ] |
| 222 | Classify the incoming batch | Identifying new and changed records | Advanced | [ ] | [ ] | [ ] | [ ] |
| 223 | Which incremental filter misses late data? | Incremental strategies and watermarks | Advanced | [ ] | [ ] | [ ] | [ ] |
| 224 | What is the unique key of the batch? | Incremental keys and duplicates | Advanced | [ ] | [ ] | [ ] | [ ] |
| 225 | Simulate the merge | Incremental merge logic | Expert | [ ] | [ ] | [ ] | [ ] |
| 226 | Late-arriving dimension rows | Late-arriving dimensions | Expert | [ ] | [ ] | [ ] | [ ] |
| 227 | Reconcile monthly totals: header versus lines | Data reconciliation | Expert | [ ] | [ ] | [ ] | [ ] |
| 228 | A reusable label-mapping table | Reusable transformation logic | Expert | [ ] | [ ] | [ ] | [ ] |
| 229 | Daily category sales mart | Reporting-ready dataset | Expert | [ ] | [ ] | [ ] | [ ] |
| 230 | dbt-style data tests in SQL | Data tests | Expert | [ ] | [ ] | [ ] | [ ] |

## Level 10: Advanced Business Scenarios and Interview Challenges (Questions 231–250)

| # | Question | Topic | Difficulty | Attempted | Solved independently | Needed hints | Revisit |
|---:|---|---|---|:---:|:---:|:---:|:---:|
| 231 | Customer lifetime value, net of refunds, with rank | CLV + ranking | Advanced | [ ] | [ ] | [ ] | [ ] |
| 232 | Multi-category shoppers | Distinct counts across a join chain | Advanced | [ ] | [ ] | [ ] | [ ] |
| 233 | Second purchase date for every customer | Nth event per group | Advanced | [ ] | [ ] | [ ] | [ ] |
| 234 | Rolling 30-day revenue | Rolling window on a date spine | Advanced | [ ] | [ ] | [ ] | [ ] |
| 235 | Three consecutive months of increasing spend | Consecutive-period pattern | Expert | [ ] | [ ] | [ ] | [ ] |
| 236 | Cohort cumulative revenue per customer | Cohort LTV curve | Expert | [ ] | [ ] | [ ] | [ ] |
| 237 | Products making up 80% of revenue in each top-level category | Pareto within groups | Expert | [ ] | [ ] | [ ] | [ ] |
| 238 | Orders placed but never paid | Anti-join on a derived status | Expert | [ ] | [ ] | [ ] | [ ] |
| 239 | Net revenue after discounts and returns | Revenue waterfall | Expert | [ ] | [ ] | [ ] | [ ] |
| 240 | Inventory shortage versus sales velocity | Days of cover | Expert | [ ] | [ ] | [ ] | [ ] |
| 241 | Reconcile orders with payments and refunds | Financial reconciliation | Expert | [ ] | [ ] | [ ] | [ ] |
| 242 | Customer-level analytical dataset | Feature table / mart | Expert | [ ] | [ ] | [ ] | [ ] |
| 243 | Rule-based suspicious transaction score | Anomaly detection with SQL | Expert | [ ] | [ ] | [ ] | [ ] |
| 244 | Monthly product performance dataset | Product x month mart | Expert | [ ] | [ ] | [ ] | [ ] |
| 245 | RFM scoring | Segmentation with NTILE | Expert | [ ] | [ ] | [ ] | [ ] |
| 246 | Time to first purchase by signup quarter | Cohort timing | Expert | [ ] | [ ] | [ ] | [ ] |
| 247 | Customers who switched favourite category | Year-over-year behaviour change | Expert | [ ] | [ ] | [ ] | [ ] |
| 248 | Does a heavy discount on the first order drive repeat purchases? | Experiment-style analysis | Expert | [ ] | [ ] | [ ] | [ ] |
| 249 | Return hot-spots | Multi-step product quality analysis | Expert | [ ] | [ ] | [ ] | [ ] |
| 250 | Monthly executive scorecard | KPI dataset | Expert | [ ] | [ ] | [ ] | [ ] |

## Reflection log

| Date | Questions covered | What I learnt | What still confuses me |
|---|---|---|---|
| | | | |
