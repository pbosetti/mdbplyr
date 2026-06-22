# Groundwork: the mock executor must understand the accumulators that later
# phases will emit, so their compiled $group stages can be asserted against a
# tibble without a live MongoDB server.

test_that("mock executor evaluates standard-deviation accumulators", {
  data <- tibble::tibble(x = c(2, 4, 4, 4, 5, 5, 7, 9))
  pipeline <- list(
    list(`$group` = list(
      `_id` = NULL,
      sample_sd = list(`$stdDevSamp` = "$x"),
      pop_sd = list(`$stdDevPop` = "$x")
    ))
  )

  result <- run_pipeline(data, pipeline)

  expect_equal(result$sample_sd, stats::sd(data$x))
  expect_equal(result$pop_sd, 2)
})

test_that("mock executor evaluates first and last accumulators", {
  data <- tibble::tibble(x = c(10, 20, 30))
  pipeline <- list(
    list(`$group` = list(
      `_id` = NULL,
      f = list(`$first` = "$x"),
      l = list(`$last` = "$x")
    ))
  )

  result <- run_pipeline(data, pipeline)

  expect_equal(result$f, 10)
  expect_equal(result$l, 30)
})

test_that("mock executor evaluates addToSet accumulator", {
  data <- tibble::tibble(x = c(1, 1, 2, 3, 3))
  pipeline <- list(
    list(`$group` = list(
      `_id` = NULL,
      s = list(`$addToSet` = "$x")
    ))
  )

  result <- run_pipeline(data, pipeline)

  expect_setequal(unlist(result$s[[1]]), c(1, 2, 3))
})

test_that("mock executor supports computed group keys", {
  data <- tibble::tibble(x = c(1, 4, 6, 9))
  pipeline <- list(
    list(`$group` = list(
      `_id` = list(parity = list(`$mod` = list("$x", 2))),
      n = list(`$sum` = 1L)
    ))
  )

  result <- run_pipeline(data, pipeline)
  buckets <- vapply(result$`_id`, function(id) id$parity, numeric(1))

  expect_setequal(buckets, c(0, 1))
  expect_equal(sum(result$n), 4)
})
