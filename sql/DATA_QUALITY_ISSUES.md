# Intentional Data Quality Issues (⚠️ spoilers for Level 8)

The dataset is synthetic, but the problems below were put there **on purpose**, because real source data is never
clean. Core relationships are kept intact (every order has a customer, every order line has an order and a product,
every payment has an order, every return points at an order line, every product has a category), so joins behave
realistically. The issues live in specific columns and records.

> **Spoiler warning.** Part of the fun of Levels 7–9 is *discovering* these problems with SQL. If you want that
> experience, attempt Level 8 first and only read this file afterwards. Rates are approximate and the exact counts are
> deliberately not listed — measuring them is the exercise.

## How to read the tables below

| Type | Meaning |
|---|---|
| **Text noise** | Leading/trailing spaces, UPPER / lower / Mixed case, alternate spellings |
| **Missing** | NULL where a value is expected |
| **Duplicate** | Same real-world fact stored more than once |
| **Invalid** | Value outside any sensible range, or breaks a business rule |
| **Orphan / gap** | A relationship that should exist but does not (or vice-versa) |
| **Timing** | Late-arriving or out-of-order records |

## Per table

### `customers`
| Column(s) | Type | What you will find | Rough rate |
|---|---|---|---|
| `email` | Missing / Invalid / Text noise | NULLs, upper-cased addresses, padded addresses, addresses without `@` | 4 % NULL, ~10 % otherwise messy |
| `first_name`, `last_name` | Text noise | stray spaces, ALL CAPS, all lower | 3–6 % |
| `gender` | Text noise / Missing | `F`/`M`/`NB`, lower/upper case variants, NULL | ~12 % non-canonical |
| `date_of_birth` | Missing / Invalid | NULL, placeholder `1900-01-01`, a few future dates | ~2 % NULL, <1 % invalid |
| `city` | Text noise | `mumbai`, `MUMBAI`, `  Pune `, plus real-world aliases: Bangalore (Bengaluru), Bombay (Mumbai), Gurgaon (Gurugram), New Delhi (Delhi), Calcutta (Kolkata), Madras (Chennai) | ~8–10 % |
| `state` | Missing | NULL | ~1 % |
| `customer_segment` | Text noise / Missing | `gold`, `GOLD`, NULL | ~5 % |
| whole row | **Duplicate** | ~180 people registered a second account later (same person and email, new `customer_id`, later `signup_date`) | 1.5 % |
| `is_active` | Invalid-ish | a CRM flag that does not always match real behaviour (customers flagged active who stopped buying, etc.) | — |

### `products`, `categories`, `warehouses`
| Column(s) | Type | What you will find |
|---|---|---|
| `product_name` | Text noise | padded names, ALL CAPS names |
| `brand` | Missing / Text noise | NULL brands, lower/upper-case brands |
| `unit_cost` vs `selling_price` | Invalid | ~10 products with cost above price (negative margin) |
| `is_active`, `discontinued_date` | Inconsistent | a few products inactive without a date, and a few active products with a discontinuation date |
| products without sales | Orphan / gap | ~3 % of the catalogue (all newer products) never sold ("dead stock") |
| `categories` | Structure | a 3-level tree: leaf products can sit 1, 2 or 3 levels below a top-level category |

### `orders`
| Column(s) | Type | What you will find | Rough rate |
|---|---|---|---|
| `payment_method` | Text noise | lower-case / trailing-space variants | ~4.5 % |
| `shipping_address` | Missing | NULL | 2 % |
| `discount_amount` | Missing | NULL (should be treated as 0 or recomputed) | 0.5 % |
| `total_amount` | Invalid | header total ≠ items + tax + shipping (5–20 % off) | 0.3 % |
| `order_status` | Business states | all seven statuses; some orders stuck in Pending/Processing/Shipped for a long time | ~3 % stuck |
| double submissions | **Duplicate** | the same customer, same basket, a few seconds apart (often one copy later cancelled) | 0.25 % |
| `loaded_at` | **Timing** | records that arrive 3–20 days after `updated_at` | 3 % |
| orders without lines | Orphan / gap | order headers whose line items are missing | ~25 orders |

### `order_items`
| What | Type | Detail |
|---|---|---|
| repeated lines | **Duplicate** | same order, product, quantity, price and discount, new `order_item_id` (~0.3 %) |
| price typos | Invalid | `unit_price` multiplied by 10 on a handful of lines |
| bulk quantities | Invalid / outlier | a few lines with quantity 10–24 |
| sold outside product life | Invalid | lines dated before `launch_date` or after `discontinued_date` (~0.3 %) |

### `payments`
| What | Type | Detail |
|---|---|---|
| `payment_status` | Text noise | `success`, `SUCCESS`, `failed`… (~2 %) |
| `payment_method` | Text noise | upper-case / leading-space variants (~4.5 %) |
| duplicate gateway callbacks | **Duplicate** | same `transaction_reference`, same order, a few seconds later (~0.4 % of orders) |
| reused references | Invalid | the same reference on two different orders (a few) |
| retries | Business | failed attempt(s) followed by a success; some customers have many failed attempts (card testing pattern) |
| short payments | Invalid | successful payment lower than the order total (~0.3 %) |
| delivered orders with no payment row | Orphan / gap | ~0.3 % of delivered/returned orders; cancelled COD orders legitimately have none |
| refunds | Business | cancelled prepaid orders carry status `Refunded` |

### `returns`
| What | Type | Detail |
|---|---|---|
| `return_reason` | Missing / Text noise | NULL (3 %), lower-case (2 %) |
| `returned_quantity` | Invalid | a few returns of more units than were bought |
| `refund_amount` | Invalid / Missing | a few refunds larger than the line value; NULL while `Pending`; 0 when `Rejected` |
| timing | Business | returns raised days to weeks after the order; a few approved-but-never-refunded cases |

### `inventory`, `warehouses`
| What | Type | Detail |
|---|---|---|
| `stock_quantity` | Invalid | a few negative values (sync errors) |
| `reserved_quantity` | Invalid | a few rows reserve more than is on hand |
| `last_updated_at` | Timing | ~28 % of rows are months stale |
| products with no inventory row | Orphan / gap | ~2 % of newer products |
| stock-stressed products | Business | a slice of active products hold only days of sales cover |

### `customer_activity`
| What | Type | Detail |
|---|---|---|
| `customer_id` NULL | Business | ~20 % anonymous sessions |
| `device_type` | Text noise / Missing | `Mobile`, `MOBILE`, NULL |
| repeated events | **Duplicate** | the same event logged twice (~0.3 %) |
| tracking gaps | Business | `purchase` events do not match the orders table one-to-one |

### `order_status_history`, `customer_changes`, `orders_incoming_batch`, `dim_date`
| Table | What you will find |
|---|---|
| `customer_changes` | exact duplicate rows (~0.4 %); `received_at` up to 30 days after `change_timestamp` for ~3 % of rows (late-arriving, out-of-order) |
| `orders_incoming_batch` | the same `order_id` more than once; new orders, changed orders, unchanged re-sends, ~25 late-arriving old orders (December `order_date`, January `loaded_at`); ~30 orders for customers who are **not yet in** `customers` (late-arriving dimension) |
| `order_status_history` | a clean event log, but statuses are visited in a path depending on the final status (useful for transitions) |
| `dim_date` | a clean calendar (the calendar is the one reliable table!) |

## Things that are *not* issues (so you don't chase them)
* Customers with no orders (about 12 % of the customer table never bought).
* Payments with several attempts per order, and a `Pending`/`Failed` status.
* NULL `customer_id` in `customer_activity` (anonymous visitors).
* Different `payment_method` between an order and one of its retry payments.
* Core foreign keys: they are all valid. If you find an orphan on a core relationship, check your join first.
