test_that("predicate translation handles boolean comparisons", {
  predicate <- mdbplyr:::translate_predicate(rlang::expr(x > 1 & y <= 2))

  expect_equal(predicate$type, "boolean")
  expect_equal(predicate$fn, "and")
  expect_equal(predicate$args[[1]]$type, "comparison")
})

test_that("unsupported filter expressions fail explicitly", {
  expect_error(
    mdbplyr:::translate_predicate(rlang::expr(mean(x))),
    "does not support"
  )
})
