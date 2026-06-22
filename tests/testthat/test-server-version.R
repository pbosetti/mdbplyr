fake_versioned_collection <- function(version = NULL) {
  collection <- list(
    name = "orders",
    aggregate = function(pipeline_json, iterate = FALSE, ...) tibble::tibble()
  )
  if (!is.null(version)) {
    collection$run <- function(command, ...) list(version = version)
  }
  collection
}

test_that("explicit server_version is parsed and reported", {
  src <- mongo_src(
    fake_versioned_collection(),
    schema = c("status", "amount"),
    server_version = "7.0"
  )

  expect_equal(mongo_server_version(src), numeric_version("7.0"))
})

test_that("server version is probed from the connected server", {
  src <- mongo_src(fake_versioned_collection("6.0.5"), schema = "amount")

  expect_equal(mongo_server_version(src), numeric_version("6.0.5"))
})

test_that("unknown server version reports NULL for test doubles", {
  src <- mongo_src(fake_versioned_collection(), schema = "amount")

  expect_null(mongo_server_version(src))
})

test_that("invalid explicit server_version fails clearly", {
  expect_error(
    mongo_src(fake_versioned_collection(), schema = "amount", server_version = "not-a-version"),
    class = "mongo_tidy_invalid"
  )
})

test_that("mongo_server_version reads through tbl_mongo", {
  tbl <- tbl_mongo(fake_versioned_collection(), schema = "amount", server_version = "5.0")

  expect_equal(mongo_server_version(tbl), numeric_version("5.0"))
})

test_that("require_server_version rejects servers that are too old", {
  src <- mongo_src(fake_versioned_collection("5.0"), schema = "amount")

  expect_error(
    require_server_version(src, "7.0", "median()"),
    class = "mongo_tidy_unsupported"
  )
})

test_that("require_server_version passes when the server is new enough", {
  src <- mongo_src(fake_versioned_collection("7.0.2"), schema = "amount")

  expect_null(require_server_version(src, "7.0", "median()"))
})

test_that("require_server_version tolerates unknown versions by default", {
  src <- mongo_src(fake_versioned_collection(), schema = "amount")

  expect_null(require_server_version(src, "7.0", "median()"))
  expect_error(
    require_server_version(src, "7.0", "median()", allow_unknown = FALSE),
    class = "mongo_tidy_unsupported"
  )
})
