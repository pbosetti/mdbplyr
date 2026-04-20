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
