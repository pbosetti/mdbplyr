test_that("compile_pipeline orders stages conservatively", {
  tbl <- mock_tbl(tibble::tibble(x = 1:5, grp = c("a", "a", "b", "b", "b"))) |>
    dplyr::filter(x > 1) |>
    dplyr::mutate(y = x * 2) |>
    dplyr::select(grp, y) |>
    dplyr::group_by(grp) |>
    dplyr::summarise(total = sum(y), n = n()) |>
    dplyr::arrange(dplyr::desc(total)) |>
    dplyr::slice_head(n = 1)

  pipeline <- compile_pipeline(tbl)
  stage_names <- vapply(pipeline, names, character(1))

  expect_equal(stage_names, c("$match", "$addFields", "$project", "$group", "$project", "$sort", "$limit"))
  expect_equal(pipeline[[4]]$`$group`$total, list(`$sum` = "$y"))
})

test_that("append_stage appends manual stages after generated pipeline", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3, y = 4:6)) |>
    dplyr::filter(x > 1) |>
    append_stage("{\"$limit\": 1}")

  pipeline <- compile_pipeline(tbl)
  stage_names <- vapply(pipeline, names, character(1))

  expect_equal(stage_names, c("$match", "$limit"))
  expect_equal(pipeline[[2]], list(`$limit` = 1))
})

test_that("slice_tail compiles through array slicing stages", {
  tbl <- mock_tbl(tibble::tibble(x = 1:5)) |>
    dplyr::slice_tail(n = 2)

  pipeline <- compile_pipeline(tbl)
  stage_names <- vapply(pipeline, names, character(1))

  expect_equal(stage_names, c("$group", "$project", "$unwind", "$replaceRoot"))
  expect_equal(pipeline[[1]]$`$group`$`__mdbplyr_slice_docs__`, list(`$push` = "$$ROOT"))
  expect_equal(
    pipeline[[2]]$`$project`$`__mdbplyr_slice_docs__`,
    list(`$slice` = list("$__mdbplyr_slice_docs__", -2L))
  )
})

test_that("negative slice_head compiles through array slicing stages", {
  tbl <- mock_tbl(tibble::tibble(x = 1:5)) |>
    dplyr::slice_head(n = -2)

  pipeline <- compile_pipeline(tbl)
  stage_names <- vapply(pipeline, names, character(1))

  expect_equal(stage_names, c("$group", "$project", "$unwind", "$replaceRoot"))
  expect_equal(
    pipeline[[2]]$`$project`$`__mdbplyr_slice_docs__`,
    list(`$slice` = list(
      "$__mdbplyr_slice_docs__",
      list(`$max` = list(
        0L,
        list(`$subtract` = list(
          list(`$size` = "$__mdbplyr_slice_docs__"),
          2L
        ))
      ))
    ))
  )
})

test_that("slice_head and slice_tail compose in call order", {
  tbl <- mock_tbl(tibble::tibble(x = 1:10)) |>
    dplyr::slice_head(n = 6) |>
    dplyr::slice_tail(n = 2)

  pipeline <- compile_pipeline(tbl)
  stage_names <- vapply(pipeline, names, character(1))

  expect_equal(stage_names, c("$limit", "$group", "$project", "$unwind", "$replaceRoot"))
  expect_equal(pipeline[[1]], list(`$limit` = 6L))
  expect_equal(
    pipeline[[3]]$`$project`$`__mdbplyr_slice_docs__`,
    list(`$slice` = list("$__mdbplyr_slice_docs__", -2L))
  )
})

test_that("mutate sequence compiles as a row-order operation", {
  tbl <- mock_tbl(tibble::tibble(x = 1:3)) |>
    dplyr::mutate(i = 1:n())

  pipeline <- compile_pipeline(tbl)
  stage_names <- vapply(pipeline, names, character(1))

  expect_equal(stage_names, c("$group", "$unwind", "$replaceRoot"))
  expect_equal(pipeline[[2]]$`$unwind`$includeArrayIndex, "__mdbplyr_seq_idx__")
})

test_that("mutate sequence preserves call order relative to arrange", {
  before_arrange <- mock_tbl(tibble::tibble(x = 1:3)) |>
    dplyr::mutate(i = 1:n()) |>
    dplyr::arrange(dplyr::desc(x))

  after_arrange <- mock_tbl(tibble::tibble(x = 1:3)) |>
    dplyr::arrange(dplyr::desc(x)) |>
    dplyr::mutate(i = 1:n())

  expect_equal(vapply(compile_pipeline(before_arrange), names, character(1))[1:4], c("$group", "$unwind", "$replaceRoot", "$sort"))
  expect_equal(vapply(compile_pipeline(after_arrange), names, character(1))[1:4], c("$sort", "$group", "$unwind", "$replaceRoot"))
})

test_that("mutate compiles dependent assignments as ordered stages", {
  tbl <- mock_tbl(tibble::tibble(a = 1:3, b = 4:6)) |>
    dplyr::mutate(c = a + b, d = c^2)

  pipeline <- compile_pipeline(tbl)
  stage_names <- vapply(pipeline, names, character(1))

  expect_equal(stage_names, c("$addFields", "$addFields"))
  expect_equal(pipeline[[1]]$`$addFields`$c, list(`$add` = list("$a", "$b")))
  expect_equal(pipeline[[2]]$`$addFields`$d, list(`$pow` = list("$c", 2)))
})

test_that("filter inlines local values while preserving dotted field references", {
  x <- 10

  tbl <- mock_tbl(tibble::tibble(`message.measurements.Fx` = c(5, 15)), name = "force") |>
    dplyr::filter(`message.measurements.Fx` > x)

  pipeline <- compile_pipeline(tbl)

  expect_equal(
    pipeline[[1]]$`$match`$`$expr`$`$gt`,
    list("$message.measurements.Fx", 10)
  )
})

test_that("select on a dotted field preserves native nested projection by default", {
  tbl <- mock_tbl(tibble::tibble(`message.measurements.Fx` = c(5, 15)), name = "force") |>
    dplyr::select(`message.measurements.Fx`)

  pipeline <- compile_pipeline(tbl)

  expect_equal(vapply(pipeline, names, character(1)), "$project")
  expect_equal(
    pipeline[[1]]$`$project`$`message.measurements.Fx`,
    1L
  )
})

test_that("flatten_fields compiles to a projection with safe aliases", {
  collection <- mock_collection(tibble::tibble(
    id = 1,
    message = list(list(timestamp = 1, measurements = list(Fx = 2, Fy = 3)))
  ))

  tbl <- tbl_mongo(collection) |>
    infer_schema() |>
    flatten_fields()

  pipeline <- compile_pipeline(tbl)

  expect_equal(vapply(pipeline, names, character(1)), "$project")
  expect_equal(unname(pipeline[[1]]$`$project`[[1]]), 1L)
  expect_true(any(vapply(pipeline[[1]]$`$project`, function(x) identical(x, "$message.timestamp"), logical(1))))
  expect_true(any(vapply(
    pipeline[[1]]$`$project`,
    function(x) identical(x, "$message.measurements.Fx"),
    logical(1)
  )))
})

test_that("unwind_array preserves call order before downstream filter and summarise", {
  tbl <- mock_tbl(tibble::tibble(
    grp = c("a", "b"),
    items = I(list(c(1, 3), c(2, 4)))
  )) |>
    unwind_array(items) |>
    dplyr::filter(items > 2) |>
    dplyr::group_by(grp) |>
    dplyr::summarise(n = n())

  pipeline <- compile_pipeline(tbl)

  expect_equal(vapply(pipeline, names, character(1)), c("$unwind", "$match", "$group", "$project"))
  expect_equal(pipeline[[1]]$`$unwind`$path, "$items")
})

test_that("flattened visible field names stay addressable in later predicates", {
  collection <- mock_collection(tibble::tibble(
    id = 1:2,
    message = list(
      list(measurements = list(Fx = 1)),
      list(measurements = list(Fx = 3))
    )
  ))

  tbl <- tbl_mongo(collection) |>
    infer_schema() |>
    flatten_fields() |>
    dplyr::filter(`message.measurements.Fx` > 1)

  pipeline <- compile_pipeline(tbl)

  expect_equal(vapply(pipeline, names, character(1)), c("$project", "$match"))
  expect_match(render_pipeline_json(pipeline), "__mdbplyr_col_")
})
