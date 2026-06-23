# Current Limits — Feasibility Ranking & Attack Plan

> Scope: this document inspects the limits listed under **Current limits** in
> `README.md` (as of `mdbplyr` 0.3.0), ranks each by implementation
> feasibility against the existing IR/compiler architecture, and proposes a
> phased attack plan. It does **not** change behaviour — it is a planning
> artifact.

## The limits under review

From `README.md`:

1. `select()` and `rename()` currently support only explicit bare field names.
2. `mutate()` and `transmute()` require named expressions and otherwise support
   scalar expressions except for the special `1:n()` row-numbering case.
3. `group_by()` supports bare field names only.
4. `summarise()` supports only the documented aggregate functions.
5. Joins, window functions, `across()`, reshaping, and write operations are out
   of scope.

To keep the ranking actionable, limit 5 is decomposed into its constituent
features, since they differ wildly in feasibility.

## How the architecture shapes feasibility

A few facts about the current design drive every estimate below:

- **There is a real IR.** Verbs append typed ops to `ir$ops`
  (`R/verbs-*.R`), and `compile_pipeline()` / `compile_ir_op()`
  (`R/compile-pipeline.R`) translate each op into aggregation stages. New
  capabilities slot in as either (a) richer op payloads or (b) new op types.
- **Expression translation already exists and is reusable.** `translate_expr()`
  (`R/translate-expr.R`) and `compile_mongo_expr()` already turn a sizeable R
  expression subset into Mongo aggregation expressions. Anything that can be
  expressed as "translate an arbitrary scalar expression and drop it into a
  `_id` / accumulator / projection slot" is cheap, because the hard part is
  done.
- **Aggregates are a small switch.** `translate_agg()` validates against a
  hard-coded `supported` vector and `compile_agg()` maps function name →
  accumulator. Adding accumulators is additive and low-risk.
- **Schema is tracked.** `schema_fields()` / `projection_mapping()` /
  `resolve_field_sources()` give field resolution that tidyselect-style helpers
  need.
- **Explicit-failure is a design principle.** Every relaxation must keep the
  "fail loudly on the unsupported" contract (README Core Principle #4), so each
  item below includes the new error surface to preserve.

## Feasibility ranking (easiest / highest value-to-effort first)

| Rank | Item | Limit | Effort | Value | New Mongo dep |
| --- | --- | --- | --- | --- | --- |
| 1 | Computed `group_by()` keys | 3 | Low | High | none |
| 2 | More `summarise()` accumulators | 4 | Low | High | `$stdDevPop`/`$stdDevSamp` (any), `$percentile` for median (7.0+) |
| 3 | tidyselect helpers in `select()`/`rename()` | 1 | Low–Med | High | none |
| 4 | Expanded scalar functions in `mutate()` | 2 | Low (per fn) | Med | none |
| 5 | `across()` in `mutate()`/`summarise()` | 5 | Med | Med | none (builds on #3) |
| 6 | Window functions | 2, 5 | High | High | `$setWindowFields` (5.0+) |
| 7 | Joins (`$lookup`) | 5 | High | Med | none |
| 8 | Reshaping (`pivot_*`) | 5 | Very high | Low | none |
| 9 | Write operations | 5 | N/A | — | violates read-only design |

### 1. Computed `group_by()` keys — **most feasible**

`group_by.tbl_mongo()` (`R/verbs-group-summarise.R:19`) currently rejects any
expression that is not a bare symbol. But the compiler already builds `_id`
from field references in `compile_group_stage()` (`R/compile-pipeline.R:272`),
and Mongo's `$group._id` accepts **any** aggregation expression. dplyr semantics
for `group_by(bucket = floor(amount / 10))` map directly.

Why it is the easiest win: the expression machinery (`translate_expr`) and a
`_id`-as-object code path already exist. The work is plumbing a translated
expression into the group `_id` instead of only a field reference.

### 2. More `summarise()` accumulators — **additive, high value**

The supported set is just `n, sum, mean, min, max` (`R/translate-agg.R:19`).
MongoDB offers direct equivalents for several common R reductions:

| R | Mongo accumulator | Notes |
| --- | --- | --- |
| `sd()` | `$stdDevSamp` | sample SD |
| `var()` | `$stdDevSamp`² (wrap in `$pow`) | derive from SD |
| `first()` | `$first` | order-dependent |
| `last()` | `$last` | order-dependent |
| `n_distinct()` | `$addToSet` + `$size` | two-step |
| `median()` / `quantile()` | `$percentile` | **Mongo 7.0+ only** |

Each is a new entry in `translate_agg()`'s `supported` vector plus a
`compile_agg()` switch case — the same shape as the existing five. Gate
`median`/`quantile` behind a server-version check or a clear "requires MongoDB
7.0+" error to honour explicit failure.

### 3. tidyselect helpers in `select()`/`rename()` — **high value, contained**

`parse_projection()` (`R/verbs-select.R:2`) aborts on anything that is not a
symbol. The most-requested gap is tidyselect (`starts_with()`, `ends_with()`,
`contains()`, `matches()`, `everything()`, `all_of()`, `any_of()`, `where()`).

These do **not** need new pipeline semantics: they resolve to a concrete field
list against the known schema and then flow through the existing
`build_projection_shape()` path. `where()` is the exception — it needs type
predicates that the schema does not currently carry, so scope the first cut to
the name-based helpers and defer `where()` until/if schema inference records
types. Requires a known schema (already a documented precondition for
projection).

### 4. Expanded scalar functions in `mutate()` — **incremental**

The "scalar expressions only" framing is mostly already generous (see the long
list of supported math/string functions in README). Two sub-parts:

- **Relaxing "named expressions required"** is trivial (auto-name like dplyr)
  but low value and slightly muddies error messages — defer/skip.
- **Adding more scalar functions** (e.g. `str_detect`/regex, `coalesce`,
  date-part extraction `year()`/`month()`, `as.numeric`/`as.character` casts)
  is a per-function addition in `translate-expr.R`'s call dispatch. Each maps to
  a Mongo operator (`$regexMatch`, `$ifNull`, `$year`, `$convert`, …). Low risk,
  but value is incremental and demand-driven — grow the list as users ask.

### 5. `across()` — **medium, depends on #3**

`across(cols, fn)` is essentially "expand a tidyselect selection into N
generated expressions." Once #3 gives a robust column resolver, `across()` in
both `mutate()` and `summarise()` becomes a desugaring step that emits multiple
computed/aggregate ops. The fiddly parts are naming (`.names` glue) and
multi-function lists. Worth doing only after #3 lands.

### 6. Window functions — **high value, high effort**

`row_number`, `rank`, `dense_rank`, `cumsum`, `lag`/`lead`, and moving
aggregates map to MongoDB `$setWindowFields` (server **5.0+**). This is the
natural completion of the existing `1:n()` sequence machinery
(`compile_sequence_stages()` already does grouped row numbering by hand). It
needs: a new IR op type, a window-spec translator (partition = current groups,
`sortBy`, frame bounds), and a compiler stage. Significant but architecturally
clean, and it is the single highest-impact "real" feature. The hand-rolled
sequence code suggests appetite already exists.

### 7. Joins (`$lookup`) — **API-design heavy**

`$lookup` exists and is expressive, but joins are hard for *interface* reasons,
not compiler reasons: a second `tbl_mongo`/collection as the right-hand side,
join-key mapping, and—critically—Mongo `$lookup` produces an **array** subfield
that then needs `$unwind` + reshaping to look like a tidy join. Result shape and
nested-vs-flat semantics need careful design to avoid violating the
"native, not SQL-emulating" stance. Medium-low feasibility; do a design RFC
before any code.

### 8. Reshaping (`pivot_wider`/`pivot_longer`) — **awkward in Mongo**

`pivot_wider` needs `$group` + `$arrayToObject` gymnastics and—worse—**the
output columns depend on the data values**, which breaks lazy/known-schema
compilation. `pivot_longer` is closer to `$objectToArray` + `$unwind` but still
niche. Low value relative to effort; consistent with README framing
("reshaping … out of scope"). Recommend keeping out of scope.

### 9. Write operations — **out of scope by design**

The package is explicitly a **read-only analytical backend** (README Overview &
Design Position). Writes contradict the core thesis. Recommend **not**
implementing; if ever needed, it belongs in a separate package/namespace, not
here. Keep the loud "not supported" error.

## Proposed attack plan (phased)

Each phase is independently shippable and ordered by value-to-effort.

### Phase 0 — Groundwork (small) — **implemented**
- ~~Add a server-feature/version capability probe on the source so version-gated
  features (median, window functions) can fail with a precise
  "requires MongoDB X.Y+" message instead of an opaque server error.~~
  Done in `R/server-version.R`: `mongo_src(..., server_version=)` stores or
  probes the server version (via `buildInfo`), `mongo_server_version()` exposes
  it, and the internal `require_server_version()` gate raises a precise
  `mongo_tidy_unsupported` error for too-old servers (tolerating unknown
  versions by default).
- ~~Extend test fixtures (`tests/testthat/helper-mock.R`) so new ops can be
  asserted at the compiled-pipeline level without a live server.~~
  Done: the mock executor now evaluates the Phase 1 accumulators
  (`$stdDevSamp`, `$stdDevPop`, `$first`, `$last`, `$addToSet`) and already
  supports computed `$group._id` expressions.

### Phase 1 — Cheap, high-value relaxations — **implemented**
1. ~~**Computed `group_by()` keys** (rank 1): translate non-symbol group exprs via
   `translate_expr()`; thread into `compile_group_stage()` `_id`.~~ Done: named
   computed keys (e.g. `group_by(bucket = floor(amount / 10))`) translate into
   `$group._id`; unnamed computed keys fail explicitly, and `1:n()` row numbering
   after a computed key is rejected.
2. ~~**Extra accumulators** (rank 2): `sd`, `var`, `first`, `last`, `n_distinct`;
   add `median`/`quantile` behind the version gate.~~ Done: `sd`→`$stdDevSamp`,
   `var`→`$stdDevSamp` squared in the summary projection, `first`/`last`,
   `n_distinct`→`$addToSet`+`$size`, and `median`/`quantile`→`$percentile`
   (gated on MongoDB 7.0+ via `require_server_version()`).
- Deliverables: updated `translate-agg.R`, `verbs-group-summarise.R`,
  `verbs-mutate.R`, `compile-pipeline.R`; new tests; README "Supported
  expressions"/"limits" and support-matrix edits.

### Phase 2 — Selection ergonomics
3. **tidyselect helpers** in `select()`/`rename()` (rank 3), name-based first;
   defer `where()`.
4. Begin demand-driven **scalar function** additions in `mutate()` (rank 4).
- Deliverables: `parse_projection()` rework to a tidyselect resolver; tests for
  each helper; docs.

### Phase 3 — Composition
5. **`across()`** (rank 5) desugaring on top of Phase 2's resolver, in both
   `mutate()` and `summarise()`.

### Phase 4 — Big feature
6. **Window functions** (rank 6) via a new `$setWindowFields` op. Treat as its
   own milestone with an upfront mini-design covering partition/order/frame
   mapping and which dplyr window verbs are in the first cut
   (`row_number`, `rank`, `dense_rank`, `cumsum`, `lag`, `lead`).

### Phase 5 — Design-first, defer
7. **Joins**: write an RFC on right-hand-side representation and result shape
   before implementing `$lookup`.
8. **Reshaping** and **writes**: keep out of scope; document the rationale so
   the boundary is intentional, not accidental.

## Cross-cutting requirements for every phase

- Preserve **explicit failure**: each newly supported form must narrow the
  existing `abort_unsupported()` surface deliberately, and anything still
  unsupported must keep erroring clearly (no silent client-side fallback).
- Keep changes **inspectable**: `show_query()` output is the contract; add
  compiled-pipeline assertions for every new op.
- Update the **README support matrix and limits list** in lockstep so docs never
  overstate or understate capability.
</content>
</invoke>
