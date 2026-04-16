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
