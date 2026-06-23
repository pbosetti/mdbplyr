test_that("select supports name-based tidyselect helpers", {
  tbl <- mock_tbl(tibble::tibble(amount = 1, amount_tax = 2, status = "a", id = 3))

  expect_equal(
    schema_fields(dplyr::select(tbl, dplyr::starts_with("amount"))),
    c("amount", "amount_tax")
  )
  expect_equal(
    schema_fields(dplyr::select(tbl, dplyr::ends_with("tax"))),
    "amount_tax"
  )
  expect_equal(
    schema_fields(dplyr::select(tbl, dplyr::everything())),
    c("amount", "amount_tax", "status", "id")
  )
})

test_that("select supports negation and all_of()", {
  tbl <- mock_tbl(tibble::tibble(amount = 1, status = "a", id = 3))

  expect_equal(
    schema_fields(dplyr::select(tbl, -status)),
    c("amount", "id")
  )

  cols <- c("status", "id")
  expect_equal(
    schema_fields(dplyr::select(tbl, dplyr::all_of(cols))),
    c("status", "id")
  )
})

test_that("select still supports bare names and renames", {
  tbl <- mock_tbl(tibble::tibble(amount = 1, status = "a"))

  expect_equal(
    schema_fields(dplyr::select(tbl, total = amount, status)),
    c("total", "status")
  )
})

test_that("select rejects where() because column types are unknown", {
  tbl <- mock_tbl(tibble::tibble(amount = 1, status = "a"))

  expect_error(
    dplyr::select(tbl, dplyr::where(is.numeric)),
    class = "mongo_tidy_unsupported"
  )
})

test_that("tidyselect helpers compile and collect to the matched projection", {
  data <- tibble::tibble(amount = c(1, 2), amount_tax = c(3, 4), status = c("a", "b"))

  query <- mock_tbl(data) |>
    dplyr::select(dplyr::starts_with("amount"))

  pipeline <- compile_pipeline(query)
  expect_equal(vapply(pipeline, names, character(1)), "$project")
  expect_equal(names(pipeline[[1]]$`$project`)[1:2], c("amount", "amount_tax"))

  result <- collect(query)
  expect_equal(names(result), c("amount", "amount_tax"))
})

test_that("select without a known schema still accepts explicit bare names", {
  collection <- list(name = "unknown_fields")
  tbl <- tbl_mongo(collection, executor = function(pipeline, ...) tibble::tibble(x = 1))

  selected <- dplyr::select(tbl, x)

  expect_equal(schema_fields(selected), "x")
})

test_that("mutate supports coalesce()", {
  data <- tibble::tibble(a = c(1, NA, 3), b = c(10, 20, 30))

  result <- mock_tbl(data) |>
    dplyr::mutate(c = dplyr::coalesce(a, b)) |>
    collect()

  expect_equal(result$c, c(1, 20, 3))
})

test_that("coalesce compiles to nested $ifNull", {
  query <- mock_tbl(tibble::tibble(a = 1, b = 2, c = 3)) |>
    dplyr::mutate(d = dplyr::coalesce(a, b, c))

  pipeline <- compile_pipeline(query)

  expect_equal(
    pipeline[[1]]$`$addFields`$d,
    list(`$ifNull` = list("$a", list(`$ifNull` = list("$b", "$c"))))
  )
})
