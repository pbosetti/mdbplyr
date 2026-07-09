test_that("is.na predicates work against missing values", {
  tbl <- mock_tbl(tibble::tibble(x = c(1, NA, 3), y = c("a", "b", NA)))

  result <- tbl |>
    dplyr::filter(is.na(y) | x > 2) |>
    collect()

  expect_equal(result$x, 3)
})

test_that("filter() comparisons drop missing-value rows like dplyr", {
  # BSON ordering sorts missing/null below every number, so an unguarded
  # {"$lt": ["$x", 100]} would *keep* documents with no x at all. The NA
  # guard must drop them, matching dplyr, for every comparison direction.
  tbl <- mock_tbl(tibble::tibble(x = c(50, NA, 150), y = c("a", "b", "c")))

  expect_equal(collect(dplyr::filter(tbl, x < 100))$y, "a")
  expect_equal(collect(dplyr::filter(tbl, x <= 100))$y, "a")
  expect_equal(collect(dplyr::filter(tbl, x > 100))$y, "c")
  expect_equal(collect(dplyr::filter(tbl, x != 50))$y, "c")
  expect_equal(collect(dplyr::filter(tbl, x == 50))$y, "a")
})

test_that("negated comparisons drop missing-value rows like dplyr's !NA", {
  tbl <- mock_tbl(tibble::tibble(x = c(50, NA, 150), y = c("a", "b", "c")))

  # R: !(NA < 100) is NA, so filter() drops the row; MongoDB's $not would
  # instead turn the guard's null into TRUE without three-valued handling.
  expect_equal(collect(dplyr::filter(tbl, !(x < 100)))$y, "c")
})

test_that("mutate() comparisons collect missing-value rows as NA", {
  tbl <- mock_tbl(tibble::tibble(x = c(50, NA, 150)))

  result <- tbl |>
    dplyr::mutate(small = x < 100) |>
    collect()

  expect_equal(result$small, c(TRUE, NA, FALSE))
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
