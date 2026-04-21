test_that("backticked dot-path fields can be selected and filtered", {
  collection <- mock_collection(tibble::tibble(
    user = list(
      list(age = 20),
      list(age = 30),
      list(age = 40)
    ),
    score = c(1, 2, 3)
  ))
  tbl <- tbl_mongo(collection) |> infer_schema()

  result <- tbl |>
    dplyr::filter(`user.age` >= 30) |>
    dplyr::select(`user.age`, score) |>
    collect()

  expect_equal(names(result), c("user", "score"))
  expect_equal(result$score, c(2, 3))
  expect_equal(unname(vapply(result$user, function(x) x[[1]], numeric(1))), c(30, 40))
})

test_that("renaming a dotted field still yields an explicit flat column", {
  tbl <- mock_tbl(tibble::tibble(`user.age` = c(20, 30, 40), score = c(1, 2, 3)))

  result <- tbl |>
    dplyr::select(age = `user.age`, score) |>
    collect()

  expect_equal(names(result), c("age", "score"))
  expect_equal(result$age, c(20, 30, 40))
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

test_that("flatten_fields collects nested objects as flat columns", {
  collection <- mock_collection(tibble::tibble(
    id = 1:2,
    message = list(
      list(timestamp = 1, measurements = list(Fx = 2, Fy = 3)),
      list(timestamp = 4, measurements = list(Fx = 5, Fy = 6))
    )
  ))

  result <- tbl_mongo(collection) |>
    infer_schema() |>
    flatten_fields() |>
    collect()

  expect_equal(
    names(result),
    c("id", "message.timestamp", "message.measurements.Fx", "message.measurements.Fy")
  )
  expect_equal(result$`message.measurements.Fx`, c(2, 5))
})

test_that("flatten_fields supports custom output naming", {
  collection <- mock_collection(tibble::tibble(
    id = 1,
    message = list(list(timestamp = 1, measurements = list(Fx = 2)))
  ))

  result <- tbl_mongo(collection) |>
    infer_schema() |>
    flatten_fields(names_fn = function(x) gsub(".", "_", x, fixed = TRUE)) |>
    collect()

  expect_equal(names(result), c("id", "message_timestamp", "message_measurements_Fx"))
})

test_that("flatten_fields expands selected nested roots to known leaf paths", {
  collection <- mock_collection(tibble::tibble(
    message = list(
      list(timestamp = 1, measurements = list(Fx = 2, Fy = 3)),
      list(timestamp = 4, measurements = list(Fx = 5, Fy = 6))
    )
  ))

  result <- tbl_mongo(collection) |>
    infer_schema() |>
    dplyr::select(`message.timestamp`, `message.measurements`) |>
    flatten_fields() |>
    collect()

  expect_equal(
    names(result),
    c("message.timestamp", "message.measurements.Fx", "message.measurements.Fy")
  )
  expect_equal(result$`message.measurements.Fx`, c(2, 5))
  expect_equal(result$`message.measurements.Fy`, c(3, 6))
})

test_that("flattened columns remain usable in later filters", {
  collection <- mock_collection(tibble::tibble(
    id = 1:2,
    message = list(
      list(measurements = list(Fx = 1)),
      list(measurements = list(Fx = 3))
    )
  ))

  result <- tbl_mongo(collection) |>
    infer_schema() |>
    flatten_fields() |>
    dplyr::filter(`message.measurements.Fx` > 1) |>
    collect()

  expect_equal(result$id, 2)
  expect_equal(result$`message.measurements.Fx`, 3)
})

test_that("unwind_array repeats rows for array elements", {
  tbl <- mock_tbl(tibble::tibble(
    id = c(1L, 2L),
    items = I(list(c(1L, 2L), c(3L, 4L)))
  ))

  result <- tbl |>
    unwind_array(items) |>
    collect()

  expect_equal(result$id, c(1L, 1L, 2L, 2L))
  expect_equal(result$items, c(1L, 2L, 3L, 4L))
})

test_that("unwind_array composes lazily with filter and summarise", {
  tbl <- mock_tbl(tibble::tibble(
    grp = c("a", "b"),
    items = I(list(c(1L, 3L), c(2L, 4L)))
  ))

  result <- tbl |>
    unwind_array(items) |>
    dplyr::filter(items > 2) |>
    dplyr::group_by(grp) |>
    dplyr::summarise(n = n()) |>
    collect()

  expect_equal(result$grp, c("a", "b"))
  expect_equal(result$n, c(1, 1))
})

test_that("unwind_array can be followed by flatten_fields for array objects", {
  collection <- mock_collection(tibble::tibble(
    id = 1,
    items = list(list(
      list(code = "a", value = 1),
      list(code = "b", value = 2)
    ))
  ))

  result <- tbl_mongo(
    collection,
    schema = c("id", "items", "items.code", "items.value")
  ) |>
    unwind_array(items) |>
    flatten_fields(items) |>
    collect()

  expect_equal(result$id, c(1, 1))
  expect_equal(result$`items.code`, c("a", "b"))
  expect_equal(result$`items.value`, c(1, 2))
})
