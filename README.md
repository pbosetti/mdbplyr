# mdbplyr

[![CI](https://github.com/pbosetti/mdbplyr/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/pbosetti/mdbplyr/actions/workflows/ci.yaml)

<img src="media/mdbplyr.png" width="200" />

A native tidy lazy backend for MongoDB in R.

## Overview

`mdbplyr` provides a disciplined `dplyr`-style interface for read-only analytical MongoDB queries. Queries stay lazy, compile into MongoDB aggregation pipelines, and only execute at `collect()`.

The package is intentionally conservative:

- it targets MongoDB aggregation pipelines directly,
- it does not emulate SQL or extend `dbplyr`,
- it fails explicitly for unsupported semantics,
- it avoids silent client-side fallback.

## Install

The package is still in its initial development phase. While it is under testing and not available on CRAN, you can install it with:

```R
install.package("pak")
pak::pak("pbosetti/mdbplyr")
```

## Current implemented subset

### Core objects and terminals

- `mongo_src()`
- `tbl_mongo()`
- `collect()`
- `cursor()`
- `show_query()`
- `schema_fields()`
- `append_stage()`

### Supported verbs

- `filter()`
- `select()`
- `rename()`
- `mutate()`
- `transmute()`
- `arrange()`
- `group_by()`
- `summarise()`
- `slice_head()`
- `head()`
- `inner_join()`
- `left_join()`
- `semi_join()`
- `anti_join()`

### Supported expressions

- field references, including backticked dot paths such as `` `user.age` ``,
- scalar literals,
- comparison operators, including `%in%`,
- boolean operators,
- arithmetic operators, including `%%` and `^`,
- `abs()`, `sqrt()`, `log()`, `log10()`, `exp()`, `floor()`, `ceiling()`, `trunc()`, `round()`,
- `sin()`, `cos()`, `tan()`, `asin()`, `acos()`, `atan()`, `atan2()`,
- `sinh()`, `cosh()`, `tanh()`, `asinh()`, `acosh()`, `atanh()`,
- `pmin()`, `pmax()`,
- `tolower()`, `toupper()`, `nchar()`, `paste()`, `paste0()`, `substr()`, `substring()`,
- `if_else()`,
- `case_when()`,
- `is.na()`,
- `n()`, `sum()`, `mean()`, `min()`, `max()`.

### Current limits

- `select()` and `rename()` currently support only explicit bare field names,
- `mutate()` and `transmute()` require named expressions,
- `group_by()` supports bare field names only,
- `summarise()` supports only the documented aggregate functions,
- joins require both tables to be `tbl_mongo` objects backed by collections in the same MongoDB database,
- `right_join()` and `full_join()` are not supported (MongoDB has no native full outer join stage starting from the left collection),
- window functions, `across()`, reshaping, and write operations are out of scope.

## Example

```r
library(mdbplyr)
library(dplyr)

orders <- tbl_mongo(
  collection = mongolite::mongo(collection = "orders", db = "analytics"),
  schema = c("customer", "amount", "status")
)

query <- orders |>
  filter(status == "paid", amount > 0) |>
  mutate(double_amount = amount * 2) |>
  group_by(customer) |>
  summarise(total = sum(double_amount), n = n()) |>
  arrange(desc(total)) |>
  slice_head(n = 10)

show_query(query)
result <- collect(query)

iter <- cursor(query)
first_page <- iter$page(5)
```

When field metadata is not discoverable from the collection object, pass `schema = ...` to `tbl_mongo()` so that projection and rename operations can stay explicit and lazy.

### Join example

`inner_join`, `left_join`, `semi_join`, and `anti_join` compile lazily to
MongoDB `$lookup` aggregation stages. Both tables must be `tbl_mongo` objects
backed by collections in the **same** MongoDB database.

```r
orders    <- tbl_mongo(mongolite::mongo("orders",    db = "shop"),
                       schema = c("order_id", "customer_id", "amount"))
customers <- tbl_mongo(mongolite::mongo("customers", db = "shop"),
                       schema = c("customer_id", "name", "country"))

# Inner join: only orders with a matching customer record
result <- inner_join(orders, customers, by = "customer_id") |>
  select(order_id, name, amount) |>
  collect()

# Left join: all orders, NA for fields of unmatched customers
result <- left_join(orders, customers, by = "customer_id") |> collect()

# Semi join: orders that have at least one matching customer (no extra columns)
result <- semi_join(orders, customers, by = "customer_id") |> collect()

# Anti join: orders with no matching customer
result <- anti_join(orders, customers, by = "customer_id") |> collect()

# Multi-key join
inner_join(sales, budgets, by = c("year", "region")) |> collect()

# Join with a pre-filtered right table (filter is embedded in $lookup)
active_customers <- filter(customers, country == "Italy")
inner_join(orders, active_customers, by = "customer_id") |> collect()

# Inspect the generated pipeline without executing
show_query(inner_join(orders, customers, by = "customer_id"))
```

---

## Project Goal

The package should provide:

- lazy query composition,
- tidy evaluation,
- translation of supported verbs into MongoDB aggregation stages,
- query inspection,
- explicit and predictable failure for unsupported operations.

The package should **not** aim to deliver full `dplyr` compatibility over arbitrary MongoDB collections.

---

## Design Position

This project should be framed as:

> a native tidy lazy analytical backend for MongoDB.

It should **not** be framed as:

> a complete `dbplyr` equivalent for MongoDB.

MongoDB documents are not rectangular SQL tables. Nested fields, arrays, missing keys, heterogeneous schemas, and document-oriented semantics require a backend that is native to MongoDB rather than adapted from SQL assumptions.

---

## Support matrix

| Capability | Status | Notes |
| --- | --- | --- |
| Lazy query state | Supported | Verbs update internal IR only |
| Pipeline inspection | Supported | `show_query()` renders compiled JSON |
| Flat-field filters | Supported | Uses `$match` + `$expr` |
| Projection and rename | Supported with caveats | Explicit bare field names only |
| Scalar mutation | Supported | Conservative expression subset |
| Grouped summaries | Supported | `n()`, `sum()`, `mean()`, `min()`, `max()` |
| Dot-path fields | Supported with caveats | Use backticked names such as `` `user.age` `` |
| Manual pipeline stage append | Supported with caveats | `append_stage()` appends raw JSON after generated stages and does not infer schema changes |
| `inner_join` / `left_join` | Supported | Compiles to `$lookup` (correlated pipeline form, MongoDB 3.6+) |
| `semi_join` / `anti_join` | Supported | Compiles to `$lookup` + `$match` |
| `right_join` / `full_join` | Not supported | No native MongoDB equivalent; `right_join` can be rewritten as `left_join` with swapped tables |
| Window functions | Not supported | Explicitly out of scope |
| Client-side fallback | Not supported | Unsupported features error clearly |

---

## Core Technical Principles

### 1. Native MongoDB translation
Do not generate SQL. Do not emulate SQL. Translate directly into MongoDB aggregation pipelines.

### 2. Internal intermediate representation
Introduce a package-specific internal query representation between the user API and the pipeline compiler.

This is a core design requirement. It allows:

- better testing,
- better diagnostics,
- cleaner compiler logic,
- easier future extension.

### 3. Lazy semantics
All supported verbs should update query state, not execute immediately.

Execution should occur only at terminal steps such as `collect()`.

### 4. Explicit failure
Unsupported operations should fail with precise diagnostics. The package should not silently pull data locally and continue computation unless that behavior is deliberately introduced later as an opt-in mode.

### 5. Conservative semantics
Ambiguous cases should be handled conservatively and documented explicitly, especially for:

- missing fields,
- `NULL` / `NA` behavior,
- heterogeneous field types,
- nested document paths,
- ordering assumptions.

---


## Expected Translation Model

Typical verb mappings are:

- `filter()` -> `$match`
- `select()` -> `$project`
- `mutate()` -> `$addFields` or `$project`
- `arrange()` -> `$sort`
- `group_by()` + `summarise()` -> `$group`
- `slice_head()` / `head()` -> `$limit`
- `inner_join()` / `left_join()` -> `$lookup` + `$unwind` + `$replaceRoot` + `$project`
- `semi_join()` / `anti_join()` -> `$lookup` + `$match` + `$project`

This mapping should be documented, inspectable, and testable.

---

