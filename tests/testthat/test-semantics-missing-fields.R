test_that("is.na predicates work against missing values", {
  tbl <- mock_tbl(tibble::tibble(x = c(1, NA, 3), y = c("a", "b", NA)))

  result <- tbl |>
    dplyr::filter(is.na(y) | x > 2) |>
    collect()

  expect_equal(result$x, 3)
})

test_that("is.na() compiles through $ifNull, not a bare $eq against null", {
  # MongoDB's $expr operators (unlike find-style query matching) do not treat
  # a field that is entirely absent from a document as equal to an explicit
  # null: {"$eq": ["$x", null]} is false for a document missing "x" altogether.
  # $ifNull unifies "missing" and "explicit null" before the comparison, which
  # is what makes is.na() correctly detect fields that are simply absent from
  # a heterogeneous document, not only ones explicitly set to null.
  tbl <- mock_tbl(tibble::tibble(x = c(1, NA, 3)))
  compiled <- compile_mongo_expr(translate_expr(rlang::quo(is.na(x)), field_map = c(x = "x")))

  expect_equal(compiled, list(`$eq` = list(list(`$ifNull` = list("$x", NULL)), NULL)))
})
