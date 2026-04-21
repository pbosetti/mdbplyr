test_that("tbl_mongo initializes inspectable IR", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3, y = 4:6))

  expect_s3_class(tbl, "tbl_mongo")
  expect_equal(tbl$ir$filters, list())
  expect_null(tbl$ir$projection)
  expect_equal(tbl$ir$manual_stages, list())
  expect_equal(schema_fields(tbl), c("x", "y"))
})

test_that("rename requires known fields", {
  collection <- list(name = "unknown_fields")
  tbl <- tbl_mongo(collection, executor = function(pipeline, ...) tibble::tibble())

  expect_error(dplyr::rename(tbl, z = x), "requires known fields")
})

test_that("rename supports renaming an existing column", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3, y = 4:6))

  renamed <- dplyr::rename(tbl, test = x)

  expect_equal(schema_fields(renamed), c("test", "y"))
  expect_equal(
    collect(renamed),
    tibble::tibble(test = 1:3, y = 4:6)
  )
})

test_that("mutate supports overwriting an existing column with exponentiation", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3, y = 4:6))

  mutated <- dplyr::mutate(tbl, x = x^2)

  expect_equal(schema_fields(mutated), c("x", "y"))
  expect_equal(
    collect(mutated),
    tibble::tibble(x = c(1, 4, 9), y = 4:6)
  )
})

test_that("mutate supports parenthesized expressions", {
  tbl <- mock_tbl(tibble::tibble(x = c(1, 2, 4), y = c(4, 8, 16)))

  mutated <- dplyr::mutate(tbl, x = y / (x ^ 2))

  expect_equal(
    collect(mutated),
    tibble::tibble(x = c(4, 2, 1), y = c(4, 8, 16))
  )
})

test_that("cursor requires cursor-capable sources", {
  tbl <- tbl_mongo(
    list(name = "orders"),
    schema = c("x"),
    executor = function(pipeline, ...) tibble::tibble(x = 1)
  )

  expect_error(cursor(tbl), "requires a collection with an \\$aggregate")
})

test_that("tbl_mongo methods are registered on dplyr generics", {
  dplyr_ns <- asNamespace("dplyr")

  expect_identical(
    getS3method("select", "tbl_mongo", envir = dplyr_ns),
    select.tbl_mongo
  )
  expect_identical(
    getS3method("filter", "tbl_mongo", envir = dplyr_ns),
    filter.tbl_mongo
  )

  if (exists("collect", envir = dplyr_ns, mode = "function", inherits = FALSE)) {
    expect_identical(
      getS3method("collect", "tbl_mongo", envir = dplyr_ns),
      collect.tbl_mongo
    )
  }

  if (exists("show_query", envir = dplyr_ns, mode = "function", inherits = FALSE)) {
    expect_identical(
      getS3method("show_query", "tbl_mongo", envir = dplyr_ns),
      show_query.tbl_mongo
    )
  }
})

test_that("dplyr generics dispatch collect and show_query for tbl_mongo", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3, y = 4:6)) |>
    dplyr::filter(x >= 2)

  dplyr_ns <- asNamespace("dplyr")

  if (exists("collect", envir = dplyr_ns, mode = "function", inherits = FALSE)) {
    expect_equal(
      dplyr::collect(tbl),
      tibble::tibble(x = 2:3, y = 5:6)
    )
  }

  if (exists("show_query", envir = dplyr_ns, mode = "function", inherits = FALSE)) {
    expect_silent(output <- capture.output(rendered <- dplyr::show_query(tbl)))
    expect_true(length(output) > 0)
    expect_true(is.character(rendered))
  }
})

test_that("append_stage validates raw JSON stage input", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3))

  expect_error(append_stage(tbl, NA_character_), "requires a single JSON string")
  expect_error(append_stage(tbl, "{\"x\": 1}"), "operator key starting with '\\$'")
  expect_error(append_stage(tbl, "[{\"$match\":{\"x\":1}}]"), "requires a single MongoDB stage object")
})

test_that("infer_schema updates a tbl_mongo from the first source document", {
  collection <- mock_collection(tibble::tibble(
    id = 1:2,
    message = list(
      list(timestamp = 1, measurements = list(Fx = 1, Fy = 2)),
      list(timestamp = 2, measurements = list(Fx = 3, Fy = 4))
    )
  ))
  collection$data <- NULL

  tbl <- tbl_mongo(collection)
  inferred <- infer_schema(tbl)

  expect_equal(
    schema_fields(inferred),
    c("id", "message.timestamp", "message.measurements.Fx", "message.measurements.Fy")
  )
  expect_equal(inferred$src$schema, schema_fields(inferred))
})

test_that("infer_schema fails for empty collections", {
  collection <- mock_collection(tibble::tibble(x = numeric()))
  collection$data <- NULL

  tbl <- tbl_mongo(collection)

  expect_error(infer_schema(tbl), "empty collection")
})
