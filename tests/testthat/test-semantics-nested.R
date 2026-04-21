test_that("backticked dot-path fields can be selected and filtered", {
  tbl <- mock_tbl(tibble::tibble(`user.age` = c(20, 30, 40), score = c(1, 2, 3)))

  result <- tbl |>
    dplyr::filter(`user.age` >= 30) |>
    dplyr::select(`user.age`, score) |>
    collect()

  expect_equal(result$`user.age`, c(30, 40))
  expect_equal(names(result), c("user.age", "score"))
})

test_that("show_query renders stable JSON", {
  tbl <- mock_tbl(tibble::tibble(x = 1:2, y = 3:4)) |>
    dplyr::mutate(z = x + y) |>
    dplyr::select(z)

  rendered <- show_query(tbl)

  expect_match(rendered, "\\$addFields")
  expect_match(rendered, "\\$project")
})

test_that("show_query renders comparison predicates with array syntax", {
  tbl <- mock_tbl(tibble::tibble(`message.measurements.Fx` = c(-1, 2), score = c(1, 2))) |>
    dplyr::filter(`message.measurements.Fx` > 0) |>
    dplyr::select(`message.measurements.Fx`, score)

  rendered <- show_query(tbl)

  expect_match(rendered, "\\$gt\"\\s*:\\s*\\[")
  expect_no_match(rendered, "\"1\"\\s*:")
})

test_that("show_query renders POSIXct literals as ISODate", {
  t0 <- as.POSIXct("2020-01-01 00:00:00", tz = "UTC")
  tbl <- mock_tbl(tibble::tibble(`message.timestamp` = t0 + c(0, 20))) |>
    dplyr::filter(`message.timestamp` > t0 + 10)

  rendered <- show_query(tbl)

  expect_match(rendered, "ISODate\\(\"2020-01-01T00:00:10.000Z\"\\)")
})
