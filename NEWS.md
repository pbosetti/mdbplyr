# mdbplyr 0.4.0

This release expands the lazy MongoDB translation layer introduced in
`v0.3.0`, moving `mdbplyr` from a small core verb subset to a broader analytical
backend with joins, window functions, richer summaries, and schema-aware
selection helpers.

## New verbs and query translations

- Added `inner_join()`, `left_join()`, `semi_join()`, and `anti_join()` methods
  for `tbl_mongo`.
- Joins compile to MongoDB `$lookup` pipelines. Mutating joins flatten matches by
  default with `$lookup`, `$unwind`, and `$replaceRoot`; `unnest = FALSE` keeps
  matches as a nested array column.
- Filtering joins keep the left-hand columns and filter rows by whether a match
  exists.
- Join support is intentionally conservative: the right-hand side must be a
  plain `tbl_mongo` collection reference with a known schema in the same
  database.

## Expanded `select()` and expression support

- `select()` now supports name-based tidyselect helpers when the schema is
  known: `starts_with()`, `ends_with()`, `contains()`, `matches()`,
  `everything()`, `all_of()`, `any_of()`, ranges, and negation.
- `select()` still rejects `where()` because column types are unknown without
  reading data.
- `coalesce()` is now translated in scalar expressions and compiles to nested
  MongoDB `$ifNull` expressions.
- `row_number()` is available as an alias for the existing `1:n()`
  row-numbering idiom in `mutate()` and `transmute()`.

## Grouping and summaries

- `group_by()` now supports named computed grouping keys, such as
  `group_by(bucket = floor(amount / 10))`.
- `summarise()` gained `sd()`, `var()`, `first()`, `last()`, and
  `n_distinct()`.
- `median()` and `quantile()` are supported on MongoDB 7.0 or newer through
  MongoDB's percentile accumulator.
- Summary expressions now use the same field-vs-local name resolution rules as
  other translated expressions.

## `across()`

- Added `across()` support in `mutate()` and `summarise()`.
- Supported selections are name-based and schema-aware, matching the new
  `select()` helper support.
- Supported functions are bare function names and `~` lambdas, with optional
  `.names` templates.
- `across()` deliberately does not support `where()`, anonymous functions, or
  functions stored in variables.

## Window functions

- Added window function translation in `mutate()` and `transmute()` via
  MongoDB `$setWindowFields`.
- Supported ranking functions are `rank()`, `min_rank()`, and `dense_rank()`.
- Supported cumulative functions are `cumsum()`, `cummean()`, `cummax()`, and
  `cummin()`.
- Supported offset functions are `lag()` and `lead()`.
- Window functions require MongoDB 5.0 or newer. Ranking functions sort by their
  column argument; cumulative and offset windows take their order from a
  preceding `arrange()`.

## Server version awareness

- `mongo_src()` and `tbl_mongo()` now accept an optional `server_version`
  argument.
- When possible, `mongo_src()` probes the connected MongoDB server with
  `buildInfo`.
- Added `mongo_server_version()` for `mongo_src` and `tbl_mongo` objects.
- Version-gated features now fail clearly on servers that are known to be too
  old.

## Behavior fixes and internals

- `mutate()` assignments now compile in user order, so later expressions can
  refer to fields created earlier in the same call.
- Fixed a `select()` regression involving nested root paths.
- Pipeline compilation now preserves operation order more explicitly through the
  internal query representation, which is needed for ordered mutation,
  row-numbering, window functions, and joins.
- Added extensive mock-backed tests for the new phases and for flat, nested, and
  missing-field semantics.

## Documentation and packaging

- The README and vignette now document the v0.4.0 feature set, limitations, and
  translation model.
- Installation instructions were updated for CRAN availability while preserving
  GitHub installation instructions for development builds.
- Generated vignette artifacts were removed from the source repository.
