# Tests for join verbs (inner_join, left_join, semi_join, anti_join)

# ---- helpers ----------------------------------------------------------------

# Small data sets used throughout the tests.
make_orders <- function(db = NULL) {
  data <- tibble::tibble(order_id = 1:4, amount = c(10, 20, 30, 40))
  if (is.null(db)) mock_tbl(data, name = "orders") else db$orders
}

make_customers <- function(db = NULL) {
  data <- tibble::tibble(order_id = c(1L, 2L, 3L), customer = c("Alice", "Bob", "Charlie"))
  if (is.null(db)) mock_tbl(data, name = "customers") else db$customers
}

make_db <- function() {
  mock_db(
    orders    = tibble::tibble(order_id = 1:4, amount = c(10, 20, 30, 40)),
    customers = tibble::tibble(order_id = c(1L, 2L, 3L), customer = c("Alice", "Bob", "Charlie"))
  )
}

# ---- parse_join_by ----------------------------------------------------------

test_that("parse_join_by uses common schema when by = NULL", {
  db  <- make_db()
  res <- dplyr::inner_join(db$orders, db$customers, by = "order_id")
  expect_equal(res$ir$join$local_keys,   "order_id")
  expect_equal(res$ir$join$foreign_keys, "order_id")
})

test_that("parse_join_by handles named character vector", {
  db     <- make_db()
  orders <- dplyr::rename(db$orders, oid = order_id)
  # Build a y with a different join-key name
  res <- dplyr::inner_join(orders, db$customers, by = c("oid" = "order_id"))
  expect_equal(res$ir$join$local_keys,   "oid")
  expect_equal(res$ir$join$foreign_keys, "order_id")
})

test_that("join errors when by = NULL and no common schema", {
  x <- mock_tbl(tibble::tibble(a = 1), name = "x")
  y <- mock_tbl(tibble::tibble(b = 2), name = "y")
  expect_error(
    dplyr::inner_join(x, y),
    class = "mongo_tidy_invalid"
  )
})

test_that("join errors when y is not a tbl_mongo", {
  db <- make_db()
  expect_error(
    dplyr::inner_join(db$orders, tibble::tibble(order_id = 1L)),
    class = "mongo_tidy_unsupported"
  )
})

test_that("right_join and full_join are unsupported", {
  db <- make_db()
  expect_error(dplyr::right_join(db$orders, db$customers, by = "order_id"),
               class = "mongo_tidy_unsupported")
  expect_error(dplyr::full_join(db$orders, db$customers, by = "order_id"),
               class = "mongo_tidy_unsupported")
})

# ---- compile_pipeline – structure -------------------------------------------

test_that("inner_join compiles $lookup + $unwind + $replaceRoot + $project", {
  db <- make_db()
  q  <- dplyr::inner_join(db$orders, db$customers, by = "order_id")
  pl <- compile_pipeline(q)

  stage_names <- vapply(pl, function(s) names(s)[[1L]], character(1L))
  expect_equal(stage_names, c("$lookup", "$unwind", "$replaceRoot", "$project"))
})

test_that("left_join compiles $lookup with preserveNullAndEmptyArrays in $unwind", {
  db <- make_db()
  q  <- dplyr::left_join(db$orders, db$customers, by = "order_id")
  pl <- compile_pipeline(q)

  stage_names <- vapply(pl, function(s) names(s)[[1L]], character(1L))
  expect_equal(stage_names, c("$lookup", "$unwind", "$replaceRoot", "$project"))

  unwind_spec <- pl[[2L]]$`$unwind`
  expect_true(is.list(unwind_spec))
  expect_true(isTRUE(unwind_spec$preserveNullAndEmptyArrays))
})

test_that("semi_join compiles $lookup + $match ($expr $size > 0) + $project", {
  db <- make_db()
  q  <- dplyr::semi_join(db$orders, db$customers, by = "order_id")
  pl <- compile_pipeline(q)

  stage_names <- vapply(pl, function(s) names(s)[[1L]], character(1L))
  expect_equal(stage_names, c("$lookup", "$match", "$project"))

  match_spec <- pl[[2L]]$`$match`
  expect_equal(match_spec$`$expr`$`$gt`[[1L]]$`$size`, "$__mdbplyr_joined__")
  expect_equal(match_spec$`$expr`$`$gt`[[2L]], 0L)
})

test_that("anti_join compiles $lookup + $match ($expr $size == 0) + $project", {
  db <- make_db()
  q  <- dplyr::anti_join(db$orders, db$customers, by = "order_id")
  pl <- compile_pipeline(q)

  stage_names <- vapply(pl, function(s) names(s)[[1L]], character(1L))
  expect_equal(stage_names, c("$lookup", "$match", "$project"))

  match_spec <- pl[[2L]]$`$match`
  expect_equal(match_spec$`$expr`$`$eq`[[1L]]$`$size`, "$__mdbplyr_joined__")
  expect_equal(match_spec$`$expr`$`$eq`[[2L]], 0L)
})

test_that("$lookup stage uses correct from / let / pipeline", {
  db <- make_db()
  q  <- dplyr::inner_join(db$orders, db$customers, by = "order_id")
  pl <- compile_pipeline(q)

  lookup <- pl[[1L]]$`$lookup`
  expect_equal(lookup$from, "customers")
  expect_equal(lookup$as,   "__mdbplyr_joined__")
  expect_true(!is.null(lookup$let))
  expect_true(!is.null(lookup$pipeline))
})

test_that("multi-key join embeds $and in the match expression", {
  db <- mock_db(
    sales    = tibble::tibble(year = c(2023L, 2023L, 2024L), region = c("A", "B", "A"), revenue = 1:3),
    budgets  = tibble::tibble(year = c(2023L, 2024L), region = c("A", "A"), budget = c(100, 200))
  )
  q  <- dplyr::inner_join(db$sales, db$budgets, by = c("year", "region"))
  pl <- compile_pipeline(q)

  embedded_match <- pl[[1L]]$`$lookup`$pipeline[[1L]]$`$match`$`$expr`
  expect_true("$and" %in% names(embedded_match))
})

test_that("join schema is updated correctly", {
  db <- make_db()

  inner <- dplyr::inner_join(db$orders, db$customers, by = "order_id")
  expect_setequal(inner$ir$schema, c("order_id", "amount", "customer"))

  semi <- dplyr::semi_join(db$orders, db$customers, by = "order_id")
  expect_setequal(semi$ir$schema, c("order_id", "amount"))
})

# ---- show_query -------------------------------------------------------------

test_that("show_query works for join queries", {
  db  <- make_db()
  q   <- dplyr::inner_join(db$orders, db$customers, by = "order_id")
  out <- capture.output(show_query(q))
  expect_true(any(grepl("\\$lookup", out)))
})

# ---- execution via mock_db --------------------------------------------------

test_that("inner_join returns only matching rows", {
  db     <- make_db()
  result <- dplyr::inner_join(db$orders, db$customers, by = "order_id") |> collect()

  expect_equal(nrow(result), 3L)
  expect_setequal(result$order_id, 1:3)
  expect_true("customer" %in% names(result))
  expect_true("amount"   %in% names(result))
  expect_false("__mdbplyr_joined__" %in% names(result))
})

test_that("inner_join result has correct values", {
  db     <- make_db()
  result <- dplyr::inner_join(db$orders, db$customers, by = "order_id") |>
    collect() |>
    dplyr::arrange(order_id)

  expect_equal(result$customer, c("Alice", "Bob", "Charlie"))
  expect_equal(result$amount,   c(10, 20, 30))
})

test_that("left_join keeps all left rows", {
  db     <- make_db()
  result <- dplyr::left_join(db$orders, db$customers, by = "order_id") |> collect()

  expect_equal(nrow(result), 4L)
})

test_that("left_join has NA for unmatched right rows", {
  db     <- make_db()
  result <- dplyr::left_join(db$orders, db$customers, by = "order_id") |>
    collect() |>
    dplyr::arrange(order_id)

  # order_id 4 has no matching customer
  expect_true(is.na(result$customer[result$order_id == 4L]))
})

test_that("semi_join returns only matching left rows", {
  db     <- make_db()
  result <- dplyr::semi_join(db$orders, db$customers, by = "order_id") |> collect()

  expect_equal(nrow(result), 3L)
  expect_setequal(result$order_id, 1:3)
  expect_false("customer" %in% names(result))
})

test_that("anti_join returns only non-matching left rows", {
  db     <- make_db()
  result <- dplyr::anti_join(db$orders, db$customers, by = "order_id") |> collect()

  expect_equal(nrow(result), 1L)
  expect_equal(result$order_id, 4L)
  expect_false("customer" %in% names(result))
})

# ---- join combined with other verbs -----------------------------------------

test_that("filter before join still works", {
  db <- make_db()
  result <- db$orders |>
    dplyr::filter(amount > 15) |>
    dplyr::inner_join(db$customers, by = "order_id") |>
    collect()

  expect_equal(nrow(result), 2L)
  expect_setequal(result$order_id, c(2L, 3L))
})

test_that("join followed by sort and limit works", {
  db <- make_db()
  result <- db$orders |>
    dplyr::inner_join(db$customers, by = "order_id") |>
    dplyr::arrange(dplyr::desc(amount)) |>
    dplyr::slice_head(n = 2L) |>
    collect()

  expect_equal(nrow(result), 2L)
  expect_equal(result$order_id[[1L]], 3L)  # highest amount first
})

test_that("join with pre-filtered y", {
  db <- make_db()
  filtered_customers <- dplyr::filter(db$customers, order_id <= 2L)
  result <- dplyr::inner_join(db$orders, filtered_customers, by = "order_id") |>
    collect()

  expect_equal(nrow(result), 2L)
  expect_setequal(result$order_id, c(1L, 2L))
})

test_that("join with different key names works", {
  db <- mock_db(
    invoices  = tibble::tibble(inv_id = 1:3, total = c(100, 200, 300)),
    items     = tibble::tibble(invoice_id = c(1L, 2L), description = c("Pen", "Book"))
  )
  result <- dplyr::inner_join(db$invoices, db$items,
                               by = c("inv_id" = "invoice_id")) |> collect()

  expect_equal(nrow(result), 2L)
  expect_true("inv_id"      %in% names(result))
  expect_true("description" %in% names(result))
  expect_false("invoice_id" %in% names(result))  # foreign key removed
})

test_that("multi-key inner_join works", {
  db <- mock_db(
    sales   = tibble::tibble(year = c(2023L, 2023L, 2024L),
                              region = c("A", "B", "A"),
                              revenue = c(10, 20, 30)),
    budgets = tibble::tibble(year = c(2023L, 2024L),
                              region = c("A", "A"),
                              budget = c(100, 200))
  )
  result <- dplyr::inner_join(db$sales, db$budgets, by = c("year", "region")) |>
    collect() |>
    dplyr::arrange(year, region)

  expect_equal(nrow(result), 2L)
  expect_equal(result$budget, c(100, 200))
})
