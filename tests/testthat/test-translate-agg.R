test_that("aggregate translation supports n and mean", {
  n_expr <- mdbplyr:::translate_agg(rlang::expr(n()))
  mean_expr <- mdbplyr:::translate_agg(rlang::expr(mean(x, na.rm = TRUE)))

  expect_equal(n_expr$fn, "n")
  expect_true(mean_expr$na_rm)
  expect_equal(mdbplyr:::compile_agg(mean_expr), list(`$avg` = "$x"))
})

test_that("unsupported aggregate calls fail clearly", {
  expect_error(mdbplyr:::translate_agg(rlang::expr(sd(x))), "Supported summaries")
})
