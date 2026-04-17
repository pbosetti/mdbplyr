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

test_that("cursor streams pipeline results through a mongo-style iterator", {
  tbl <- mock_tbl(tibble::tibble(x = 1:4, y = c(10, 20, 30, 40)))

  iter <- tbl |>
    dplyr::filter(x >= 2) |>
    dplyr::mutate(double_y = y * 2) |>
    dplyr::arrange(x) |>
    cursor()

  expect_s3_class(iter, "mongo_iter")
  expect_equal(iter$page(2), tibble::tibble(x = 2:3, y = c(20, 30), double_y = c(40, 60)))
  expect_equal(iter$one(), list(x = 4, y = 40, double_y = 80))
  expect_equal(iter$page(), tibble::tibble(x = integer(), y = numeric(), double_y = numeric()))
})

test_that("select and transmute update the visible schema", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3, y = 4:6, z = 7:9))

  selected <- dplyr::select(tbl, y, new_x = x)
  transmuted <- dplyr::transmute(tbl, total = x + y)

  expect_equal(names(compile_pipeline(selected)[[1]]$`$project`)[1:2], c("y", "new_x"))
  expect_equal(schema_fields(transmuted), "total")
})

test_that("extended scalar operators execute through the mock pipeline", {
  tbl <- mock_tbl(tibble::tibble(
    x = c(1, 2, 3),
    y = c(2, 5, 8),
    label = c("Ab", "cD", "Ef"),
    code = c("alpha", "bravo", "charlie")
  ))

  result <- tbl |>
    dplyr::mutate(
      mod_y = y %% 3,
      is_special = x %in% c(1, 3),
      upper_label = toupper(label),
      lower_label = tolower(label),
      label_size = nchar(label),
      joined = paste(label, x, sep = "-"),
      clipped = substr(code, 2, 4),
      log_y = log10(y),
      min_xy = pmin(x, y),
      max_xy = pmax(x, y)
    ) |>
    collect()

  expect_equal(result$mod_y, c(2, 2, 2))
  expect_equal(result$is_special, c(TRUE, FALSE, TRUE))
  expect_equal(result$upper_label, c("AB", "CD", "EF"))
  expect_equal(result$lower_label, c("ab", "cd", "ef"))
  expect_equal(result$label_size, c(2L, 2L, 2L))
  expect_equal(result$joined, c("Ab-1", "cD-2", "Ef-3"))
  expect_equal(result$clipped, c("lph", "rav", "har"))
  expect_equal(result$log_y, log10(c(2, 5, 8)))
  expect_equal(result$min_xy, c(1, 2, 3))
  expect_equal(result$max_xy, c(2, 5, 8))
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
