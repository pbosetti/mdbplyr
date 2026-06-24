# mdbplyr

[![CI](https://github.com/pbosetti/mdbplyr/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/pbosetti/mdbplyr/actions/workflows/ci.yaml) 
[![Downloads](https://cranlogs.r-pkg.org/badges/mdbplyr)](https://cran.r-project.org/package=mdbplyr) 
[![CRAN status](https://www.r-pkg.org/badges/version/mdbplyr)](https://CRAN.R-project.org/package=mdbplyr) 
[![R-universe version](https://pbosetti.r-universe.dev/mdbplyr/badges/version)](https://pbosetti.r-universe.dev/mdbplyr)

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

You can install the package from CRAN with `install.package("mdbplyr")`. If you rather want the latest version from GitHub:

```R
install.package(c("devtools", "remotes", "knitr", "rmarkdown"))
remotes::install_github("pbosetti/mdbplyr", build_vignettes = TRUE)
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
- `slice_tail()`
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
- `coalesce()`,
- `across()` in `mutate()` / `summarise()` with name-based column selections and bare function names or `~` lambdas,
- window functions in `mutate()` / `transmute()` (require MongoDB 5.0+): `min_rank()` / `rank()`, `dense_rank()`, `cumsum()` / `cummean()` / `cummax()` / `cummin()`, `lag()`, `lead()` (and `row_number()` as an alias for `1:n()`),
- `1:n()` in `mutate()` / `transmute()` for row numbering,
- `n()`, `sum()`, `mean()`, `min()`, `max()`, `sd()`, `var()`, `first()`, `last()`, `n_distinct()`,
- `median()` and `quantile()` (require MongoDB 7.0+).

### Current limits

- `select()` supports bare names, renames, and name-based tidyselect helpers (`starts_with()`, `ends_with()`, `contains()`, `matches()`, `everything()`, `all_of()`, `any_of()`, ranges, negation) when the schema is known, but not `where()`; `rename()` supports explicit bare field renames only,
- `mutate()` and `transmute()` require named expressions and otherwise support scalar expressions except for the special `1:n()` row-numbering case,
- `group_by()` supports bare field names and named computed keys such as `bucket = floor(amount / 10)`,
- `summarise()` supports only the documented aggregate functions; `median()` / `quantile()` additionally require MongoDB 7.0+,
- `across()` supports name-based column selections with bare function names or `~` lambdas, but not `where()` or functions held in variables,
- window functions compile to `$setWindowFields` (MongoDB 5.0+); ranking sorts by its column argument, while cumulative and offset windows take their order from a preceding `arrange()` and reorder the output by that key,
- `inner_join()` / `left_join()` / `semi_join()` / `anti_join()` compile to `$lookup`; the right-hand side must be a plain `tbl_mongo` (a collection reference with a known schema) in the same database,
- reshaping and write operations are out of scope.

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

---


## Design Position

The package provides:

- lazy query composition,
- tidy evaluation,
- translation of supported verbs into MongoDB aggregation stages,
- query inspection,
- explicit and predictable failure for unsupported operations.

This project should be framed as:

> a native tidy lazy analytical backend for MongoDB.

It should **not** be framed as:

> a complete `dbplyr` equivalent for MongoDB.

MongoDB documents are not rectangular SQL tables. Nested fields, arrays, missing keys, heterogeneous schemas, and document-oriented semantics require a backend that is native to MongoDB rather than adapted from SQL assumptions.

For these reasons, the package can **not** aim to deliver full `dplyr` compatibility over arbitrary MongoDB collections.

---

## Support matrix

| Capability | Status | Notes |
| --- | --- | --- |
| Lazy query state | Supported | Verbs update internal IR only |
| Pipeline inspection | Supported | `show_query()` renders compiled JSON |
| Flat-field filters | Supported | Uses `$match` + `$expr` |
| Projection and rename | Supported with caveats | `select()` adds name-based tidyselect helpers (no `where()`); `rename()` is bare renames |
| Scalar mutation | Supported | Conservative expression subset |
| Grouped summaries | Supported | `n()`, `sum()`, `mean()`, `min()`, `max()`, `sd()`, `var()`, `first()`, `last()`, `n_distinct()`; `median()`/`quantile()` need MongoDB 7.0+ |
| Computed group keys | Supported | Named expressions, e.g. `group_by(bucket = floor(amount / 10))` |
| `across()` | Supported with caveats | Name-based selections; bare function names or `~` lambdas; no `where()` or function variables |
| Dot-path fields | Supported with caveats | Use backticked names such as `` `user.age` `` |
| Manual pipeline stage append | Supported with caveats | `append_stage()` appends raw JSON after generated stages and does not infer schema changes |
| Window functions | Supported with caveats | `$setWindowFields` (MongoDB 5.0+); rank/dense_rank, cum*, lag/lead; ordering via the ranking column or a preceding `arrange()` |
| Joins | Supported with caveats | `inner`/`left`/`semi`/`anti` via `$lookup`; plain right-hand `tbl_mongo`, same database; flattened by default or nested with `unnest = FALSE` |
| Reshaping / writes | Not supported | Explicitly out of scope |
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
- `slice_head()` / `head()` -> `$limit` or array-slicing stages for negative `n`
- `slice_tail()` -> array-slicing stages
- window functions in `mutate()` -> `$setWindowFields`
- `inner_join()` / `left_join()` -> `$lookup` (+ `$unwind` + `$replaceRoot`)
- `semi_join()` / `anti_join()` -> `$lookup` + `$match` on match count

This mapping should be documented, inspectable, and testable.

---

# Author

Paolo Bosetti, University of Trento, Department of Industrial Engineering <https://ror.org/05trd4x28>