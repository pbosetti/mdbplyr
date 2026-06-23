test_that("aggregate translation supports n and mean", {
  n_expr <- mdbplyr:::translate_agg(rlang::expr(n()))
  mean_expr <- mdbplyr:::translate_agg(rlang::expr(mean(x, na.rm = TRUE)))

  expect_equal(n_expr$fn, "n")
  expect_true(mean_expr$na_rm)
  expect_equal(mdbplyr:::compile_agg(mean_expr), list(`$avg` = "$x"))
})

test_that("unsupported aggregate calls fail clearly", {
  expect_error(mdbplyr:::translate_agg(rlang::expr(prod(x))), "Supported summaries")
})

test_that("aggregate translation supports the extended accumulators", {
  expect_equal(
    mdbplyr:::compile_agg(mdbplyr:::translate_agg(rlang::expr(sd(x)))),
    list(`$stdDevSamp` = "$x")
  )
  expect_equal(
    mdbplyr:::compile_agg(mdbplyr:::translate_agg(rlang::expr(first(x)))),
    list(`$first` = "$x")
  )
  expect_equal(
    mdbplyr:::compile_agg(mdbplyr:::translate_agg(rlang::expr(n_distinct(x)))),
    list(`$addToSet` = "$x")
  )
})

test_that("var and n_distinct add a follow-up summary projection transform", {
  var_spec <- mdbplyr:::translate_agg(rlang::expr(var(x)))
  expect_equal(mdbplyr:::compile_agg(var_spec), list(`$stdDevSamp` = "$x"))
  expect_equal(
    mdbplyr:::compile_summary_value("v", var_spec),
    list(`$pow` = list("$v", 2L))
  )

  nd_spec <- mdbplyr:::translate_agg(rlang::expr(n_distinct(x)))
  expect_equal(
    mdbplyr:::compile_summary_value("d", nd_spec),
    list(`$size` = "$d")
  )
})

test_that("median compiles to a percentile accumulator with array extraction", {
  median_spec <- mdbplyr:::translate_agg(rlang::expr(median(x)))
  expect_equal(
    mdbplyr:::compile_agg(median_spec),
    list(`$percentile` = list(input = "$x", p = list(0.5), method = "approximate"))
  )
  expect_equal(
    mdbplyr:::compile_summary_value("m", median_spec),
    list(`$arrayElemAt` = list("$m", 0L))
  )
})

test_that("quantile requires a single probs value in range", {
  q_spec <- mdbplyr:::translate_agg(rlang::expr(quantile(x, probs = 0.9)))
  expect_equal(
    mdbplyr:::compile_agg(q_spec),
    list(`$percentile` = list(input = "$x", p = list(0.9), method = "approximate"))
  )
  expect_error(
    mdbplyr:::translate_agg(rlang::expr(quantile(x, probs = 1.5))),
    class = "mongo_tidy_unsupported"
  )
})
