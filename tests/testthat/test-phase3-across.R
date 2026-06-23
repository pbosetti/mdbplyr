test_that("summarise across applies one aggregate to several columns", {
  data <- tibble::tibble(g = c("a", "a", "b"), x = c(1, 2, 3), y = c(10, 20, 30))

  result <- mock_tbl(data) |>
    dplyr::group_by(g) |>
    dplyr::summarise(dplyr::across(c(x, y), mean)) |>
    collect()

  result <- result[order(result$g), ]

  expect_equal(names(result), c("g", "x", "y"))
  expect_equal(result$x, c(mean(c(1, 2)), 3))
  expect_equal(result$y, c(mean(c(10, 20)), 30))
})

test_that("summarise across with a named function list uses {.col}_{.fn} names", {
  data <- tibble::tibble(g = c("a", "a", "b"), x = c(1, 2, 3), y = c(10, 20, 30))

  result <- mock_tbl(data) |>
    dplyr::group_by(g) |>
    dplyr::summarise(dplyr::across(c(x, y), list(avg = mean, total = sum))) |>
    collect()

  expect_setequal(names(result), c("g", "x_avg", "x_total", "y_avg", "y_total"))

  result <- result[order(result$g), ]
  expect_equal(result$x_avg, c(mean(c(1, 2)), 3))
  expect_equal(result$x_total, c(3, 3))
})

test_that("summarise across forwards extra arguments to the aggregate", {
  data <- tibble::tibble(a = c(1, NA, 3), b = c(4, 5, 6))

  result <- mock_tbl(data) |>
    dplyr::summarise(dplyr::across(dplyr::everything(), sum, na.rm = TRUE)) |>
    collect()

  expect_equal(result$a, 4)
  expect_equal(result$b, 15)
})

test_that("mutate across applies a lambda to each selected column", {
  data <- tibble::tibble(x = c(1, 2, 3), y = c(10, 20, 30))

  result <- mock_tbl(data) |>
    dplyr::mutate(dplyr::across(c(x, y), ~ .x * 2)) |>
    collect()

  expect_equal(result$x, c(2, 4, 6))
  expect_equal(result$y, c(20, 40, 60))
})

test_that("mutate across honours a custom .names glue", {
  data <- tibble::tibble(x = c(1, 2, 3), y = c(10, 20, 30))

  result <- mock_tbl(data) |>
    dplyr::mutate(dplyr::across(c(x, y), ~ .x + 1, .names = "{.col}_inc")) |>
    collect()

  expect_true(all(c("x_inc", "y_inc") %in% names(result)))
  expect_equal(result$x_inc, c(2, 3, 4))
  expect_equal(result$y_inc, c(11, 21, 31))
})

test_that("mutate across compiles to one $addFields stage per column", {
  query <- mock_tbl(tibble::tibble(x = 1, y = 2)) |>
    dplyr::mutate(dplyr::across(c(x, y), ~ .x * 2))

  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), c("$addFields", "$addFields"))
  expect_equal(pipeline[[1]]$`$addFields`$x, list(`$multiply` = list("$x", 2)))
  expect_equal(pipeline[[2]]$`$addFields`$y, list(`$multiply` = list("$y", 2)))
})

test_that("across without a known schema fails clearly", {
  collection <- list(name = "unknown_fields")
  tbl <- tbl_mongo(collection, executor = function(pipeline, ...) tibble::tibble())

  expect_error(
    dplyr::summarise(tbl, dplyr::across(dplyr::everything(), mean)),
    class = "mongo_tidy_invalid"
  )
})

test_that("across rejects where() selections", {
  tbl <- mock_tbl(tibble::tibble(x = 1, y = 2))

  expect_error(
    dplyr::summarise(tbl, dplyr::across(dplyr::where(is.numeric), mean)),
    class = "mongo_tidy_unsupported"
  )
})
