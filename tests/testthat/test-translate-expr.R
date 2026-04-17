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
