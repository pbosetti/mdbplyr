test_that("computed group_by keys compile into $group._id", {
  query <- mock_tbl(tibble::tibble(amount = 1:5)) |>
    dplyr::group_by(decade = floor(amount / 10)) |>
    dplyr::summarise(n = dplyr::n())

  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), c("$group", "$project"))
  expect_equal(
    pipeline[[1]]$`$group`$`_id`$decade,
    list(`$floor` = list(list(`$divide` = list("$amount", 10))))
  )
  expect_equal(pipeline[[2]]$`$project`$decade, "$_id.decade")
})

test_that("unnamed computed group keys fail explicitly", {
  tbl <- mock_tbl(tibble::tibble(amount = 1:5))

  expect_error(
    dplyr::group_by(tbl, floor(amount / 10)),
    class = "mongo_tidy_unsupported"
  )
})

test_that("computed group keys execute end to end", {
  data <- tibble::tibble(amount = c(3, 7, 12, 18, 25))
  result <- mock_tbl(data) |>
    dplyr::group_by(bucket = floor(amount / 10)) |>
    dplyr::summarise(n = dplyr::n()) |>
    collect()

  result <- result[order(result$bucket), ]

  expect_equal(result$bucket, c(0, 1, 2))
  expect_equal(result$n, c(2L, 2L, 1L))
})

test_that("row numbering after a computed group key is rejected", {
  tbl <- mock_tbl(tibble::tibble(amount = 1:5)) |>
    dplyr::group_by(bucket = floor(amount / 10))

  expect_error(
    dplyr::mutate(tbl, i = 1:dplyr::n()),
    class = "mongo_tidy_unsupported"
  )
})

test_that("extended accumulators execute per group", {
  data <- tibble::tibble(
    g = c("a", "a", "a", "b", "b"),
    x = c(1, 2, 6, 10, 14)
  )

  result <- mock_tbl(data) |>
    dplyr::group_by(g) |>
    dplyr::summarise(
      sd_x = sd(x),
      var_x = var(x),
      first_x = dplyr::first(x),
      last_x = dplyr::last(x),
      nd = dplyr::n_distinct(x)
    ) |>
    collect()

  result <- result[order(result$g), ]

  expect_equal(result$g, c("a", "b"))
  expect_equal(result$sd_x, c(stats::sd(c(1, 2, 6)), stats::sd(c(10, 14))))
  expect_equal(result$var_x, c(stats::var(c(1, 2, 6)), stats::var(c(10, 14))))
  expect_equal(result$first_x, c(1, 10))
  expect_equal(result$last_x, c(6, 14))
  expect_equal(result$nd, c(3L, 2L))
})

test_that("median compiles through a $percentile accumulator", {
  query <- mock_tbl(tibble::tibble(g = c("a", "a"), x = c(1, 2))) |>
    dplyr::group_by(g) |>
    dplyr::summarise(m = median(x))

  pipeline <- compile_pipeline(query)

  expect_equal(
    pipeline[[1]]$`$group`$m,
    list(`$percentile` = list(input = "$x", p = list(0.5), method = "approximate"))
  )
  expect_equal(
    pipeline[[2]]$`$project`$m,
    list(`$arrayElemAt` = list("$m", 0L))
  )
})

test_that("median and quantile are gated on MongoDB 7.0+", {
  collection <- mock_collection(tibble::tibble(x = c(1, 2, 3)))
  old_tbl <- tbl_mongo(collection, schema = "x", server_version = "5.0")
  new_tbl <- tbl_mongo(collection, schema = "x", server_version = "7.0")

  expect_error(
    dplyr::summarise(old_tbl, m = median(x)),
    class = "mongo_tidy_unsupported"
  )
  expect_no_error(dplyr::summarise(new_tbl, m = median(x)))
})
