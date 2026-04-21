test_that("expression translation covers scalar operators", {
  expr <- mdbplyr:::translate_expr(rlang::expr(round(abs(x) / 2, 1)))

  expect_equal(expr$type, "round")
  expect_equal(expr$digits, 1L)
  expect_equal(expr$arg$type, "call")
})

test_that("extended expression translation maps to MongoDB operators", {
  expr <- mdbplyr:::translate_expr(rlang::expr(paste(toupper(name), substr(code, 2, 4), sep = "-")))

  expect_equal(expr$type, "call")
  expect_equal(expr$fn, "concat")
  expect_equal(expr$args[[1]]$fn, "toUpper")
  expect_equal(expr$args[[3]]$fn, "substrCP")
})

test_that("case_when translation yields stable structure", {
  expr <- mdbplyr:::translate_expr(rlang::expr(case_when(x > 1 ~ "big", TRUE ~ "small")))

  expect_equal(expr$type, "case_when")
  expect_length(expr$cases, 1)
  expect_equal(expr$default$type, "literal")
})

test_that("compiled comparison operators serialize as Mongo arrays", {
  expr <- mdbplyr:::translate_expr(rlang::expr(`message.measurements.Fx` > 0), context = "predicate")

  compiled <- mdbplyr:::compile_mongo_expr(expr)
  rendered <- jsonlite::toJSON(compiled, auto_unbox = TRUE, pretty = TRUE, null = "null")

  expect_null(names(compiled$`$gt`))
  expect_match(rendered, "\\$gt\": \\[")
  expect_no_match(rendered, "\"1\":")
})

test_that("compiled extended operators keep literal vectors and array syntax", {
  expr <- mdbplyr:::translate_expr(rlang::expr(x %in% c(1, 3, 5)), context = "predicate")
  compiled <- mdbplyr:::compile_mongo_expr(expr)

  expect_equal(compiled$`$in`[[1]], "$x")
  expect_equal(compiled$`$in`[[2]], c(1, 3, 5))
  expect_null(names(compiled$`$in`))
})

test_that("bare symbols fall back to local values when not in schema", {
  x <- 10

  expr <- mdbplyr:::translate_expr(rlang::quo(amount > x), context = "predicate", fields = "amount")
  compiled <- mdbplyr:::compile_mongo_expr(expr)

  expect_equal(compiled$`$gt`, list("$amount", 10))
})

test_that("explicit .data and .env pronouns override ambiguity", {
  x <- 10

  expr <- mdbplyr:::translate_expr(rlang::quo(.data$x > .env$x), context = "predicate", fields = "x")
  compiled <- mdbplyr:::compile_mongo_expr(expr)

  expect_equal(compiled$`$gt`, list("$x", 10))
})

test_that("ambiguous bare names keep field precedence", {
  x <- 10

  expr <- mdbplyr:::translate_expr(rlang::quo(x > 1), context = "predicate", fields = "x")
  compiled <- mdbplyr:::compile_mongo_expr(expr)

  expect_equal(compiled$`$gt`, list("$x", 1))
})

test_that("local-only subexpressions inline as literals", {
  x <- 10

  expr <- mdbplyr:::translate_expr(rlang::quo(amount > x + 1), context = "predicate", fields = "amount")
  compiled <- mdbplyr:::compile_mongo_expr(expr)

  expect_equal(compiled$`$gt`, list("$amount", 11))
})

test_that("unknown bare symbols fail explicitly", {
  expect_error(
    mdbplyr:::translate_expr(rlang::quo(amount > missing_value), context = "predicate", fields = "amount"),
    "cannot evaluate local expression"
  )
})

test_that("date-time literals compile to Mongo date literals", {
  ts <- as.POSIXct("2020-01-01 00:00:10", tz = "UTC")

  expr <- mdbplyr:::translate_expr(rlang::quo(timestamp > ts), context = "predicate", fields = "timestamp")
  compiled <- mdbplyr:::compile_mongo_expr(expr)

  expect_equal(compiled$`$gt`[[1]], "$timestamp")
  expect_equal(compiled$`$gt`[[2]], list(`$date` = "2020-01-01T00:00:10.000Z"))
})
