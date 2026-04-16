test_that("flat pipelines remain lazy until collect", {
  data <- tibble::tibble(x = c(1, 2, 3, 4), y = c(10, 20, 30, 40), grp = c("a", "a", "b", "b"))
  tbl <- mock_tbl(data)

  query <- tbl |>
    dplyr::filter(x >= 2) |>
    dplyr::mutate(double_y = y * 2, label = if_else(x > 2, "big", "small")) |>
    dplyr::arrange(dplyr::desc(double_y)) |>
    dplyr::slice_head(n = 2)

  expect_equal(nrow(tbl$src$collection$data), 4)

  result <- collect(query)

  expect_equal(result$double_y, c(80, 60))
  expect_equal(result$label, c("big", "big"))
})

test_that("select and transmute update the visible schema", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3, y = 4:6, z = 7:9))

  selected <- dplyr::select(tbl, y, new_x = x)
  transmuted <- dplyr::transmute(tbl, total = x + y)

  expect_equal(names(compile_pipeline(selected)[[1]]$`$project`)[1:2], c("y", "new_x"))
  expect_equal(schema_fields(transmuted), "total")
})

test_that("append_stage participates in lazy execution", {
  tbl <- mock_tbl(tibble::tibble(x = 1:4, y = c(10, 40, 20, 30)))

  query <- tbl |>
    dplyr::arrange(dplyr::desc(y)) |>
    append_stage("{\"$limit\": 2}")

  expect_equal(nrow(tbl$src$collection$data), 4)

  result <- collect(query)

  expect_equal(result$y, c(40, 30))
})
