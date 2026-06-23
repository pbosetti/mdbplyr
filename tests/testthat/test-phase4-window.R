empty_obj <- structure(list(), names = character())

test_that("ranking window functions compile to $setWindowFields", {
  query <- mock_tbl(tibble::tibble(g = c("a", "b"), x = c(1, 2))) |>
    dplyr::group_by(g) |>
    dplyr::mutate(r = dplyr::min_rank(x))

  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), "$setWindowFields")
  swf <- pipeline[[1]]$`$setWindowFields`
  expect_equal(swf$partitionBy, "$g")
  expect_equal(swf$sortBy, list(x = 1L))
  expect_equal(swf$output$r, list(`$rank` = empty_obj))
})

test_that("dense_rank(desc()) sorts descending without a partition", {
  query <- mock_tbl(tibble::tibble(x = c(1, 2, 3))) |>
    dplyr::mutate(r = dplyr::dense_rank(dplyr::desc(x)))

  swf <- compile_pipeline(query)[[1]]$`$setWindowFields`

  expect_null(swf$partitionBy)
  expect_equal(swf$sortBy, list(x = -1L))
  expect_equal(swf$output$r, list(`$denseRank` = empty_obj))
})

test_that("multiple group keys produce a partitionBy document", {
  query <- mock_tbl(tibble::tibble(g1 = "a", g2 = "b", x = 1)) |>
    dplyr::group_by(g1, g2) |>
    dplyr::mutate(r = dplyr::min_rank(x))

  swf <- compile_pipeline(query)[[1]]$`$setWindowFields`

  expect_equal(swf$partitionBy, list(g1 = "$g1", g2 = "$g2"))
})

test_that("cumsum compiles to a windowed $sum over a preceding arrange", {
  query <- mock_tbl(tibble::tibble(t = c(1, 2, 3), v = c(10, 20, 30))) |>
    dplyr::arrange(t) |>
    dplyr::mutate(running = cumsum(v))

  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), c("$sort", "$setWindowFields"))
  swf <- pipeline[[2]]$`$setWindowFields`
  expect_equal(swf$sortBy, list(t = 1L))
  expect_equal(
    swf$output$running,
    list(`$sum` = "$v", window = list(documents = list("unbounded", "current")))
  )
})

test_that("cumulative windows require a preceding arrange()", {
  tbl <- mock_tbl(tibble::tibble(v = c(1, 2, 3)))

  expect_error(
    dplyr::mutate(tbl, running = cumsum(v)),
    class = "mongo_tidy_invalid"
  )
})

test_that("lag and lead compile to $shift with the right offsets", {
  query <- mock_tbl(tibble::tibble(t = 1:3, v = c(10, 20, 30))) |>
    dplyr::arrange(t) |>
    dplyr::mutate(prev = dplyr::lag(v), nxt = dplyr::lead(v, 2))

  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), c("$sort", "$setWindowFields", "$setWindowFields"))
  expect_equal(
    pipeline[[2]]$`$setWindowFields`$output$prev,
    list(`$shift` = list(output = "$v", by = -1L))
  )
  expect_equal(
    pipeline[[3]]$`$setWindowFields`$output$nxt,
    list(`$shift` = list(output = "$v", by = 2L))
  )
})

test_that("lag accepts a literal default", {
  query <- mock_tbl(tibble::tibble(t = 1:3, v = c(10, 20, 30))) |>
    dplyr::arrange(t) |>
    dplyr::mutate(prev = dplyr::lag(v, default = 0))

  shift <- compile_pipeline(query)[[2]]$`$setWindowFields`$output$prev$`$shift`

  expect_equal(shift$by, -1L)
  expect_equal(shift$default, 0)
})

test_that("window functions require MongoDB 5.0+", {
  collection <- mock_collection(tibble::tibble(t = 1:3, v = 1:3))
  old_tbl <- tbl_mongo(collection, schema = c("t", "v"), server_version = "4.4")

  expect_error(
    old_tbl |> dplyr::arrange(t) |> dplyr::mutate(running = cumsum(v)),
    class = "mongo_tidy_unsupported"
  )
})

test_that("row_number() is an alias for 1:n()", {
  via_row_number <- compile_pipeline(
    mock_tbl(tibble::tibble(x = 1:3)) |> dplyr::mutate(r = dplyr::row_number())
  )
  via_sequence <- compile_pipeline(
    mock_tbl(tibble::tibble(x = 1:3)) |> dplyr::mutate(r = 1:dplyr::n())
  )

  expect_equal(via_row_number, via_sequence)
})

test_that("$rank renders as an empty JSON object", {
  query <- mock_tbl(tibble::tibble(x = c(1, 2, 3))) |>
    dplyr::mutate(r = dplyr::min_rank(x))

  rendered <- render_pipeline_json(compile_pipeline(query))

  expect_match(rendered, "\"\\$rank\"\\s*:\\s*\\{\\s*\\}")
})
