people <- function() {
  mock_tbl(tibble::tibble(name = "Luke", world_id = 1, mass = 77), name = "people")
}
worlds <- function() {
  mock_tbl(tibble::tibble(world_id = 1, world_name = "Tatooine"), name = "worlds")
}

test_that("inner_join compiles to $lookup + $unwind + $replaceRoot", {
  query <- dplyr::inner_join(people(), worlds(), by = "world_id")
  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), c("$lookup", "$unwind", "$replaceRoot"))

  lookup <- pipeline[[1]]$`$lookup`
  expect_equal(lookup$from, "worlds")
  expect_equal(lookup$as, "__mdbplyr_join__")
  expect_equal(lookup$let, list(mdb_l0 = "$world_id"))
  expect_equal(
    lookup$pipeline,
    list(list(`$match` = list(`$expr` = list(`$eq` = list("$world_id", "$$mdb_l0")))))
  )

  expect_equal(pipeline[[2]]$`$unwind`, list(path = "$__mdbplyr_join__"))

  new_root <- pipeline[[3]]$`$replaceRoot`$newRoot
  expect_equal(new_root$name, "$name")
  expect_equal(new_root$world_id, "$world_id")
  expect_equal(new_root$world_name, "$__mdbplyr_join__.world_name")
  expect_setequal(schema_fields(query), c("name", "world_id", "mass", "world_name"))
})

test_that("left_join preserves unmatched left rows", {
  query <- dplyr::left_join(people(), worlds(), by = "world_id")
  pipeline <- compile_pipeline(query)

  expect_equal(
    pipeline[[2]]$`$unwind`,
    list(path = "$__mdbplyr_join__", preserveNullAndEmptyArrays = TRUE)
  )
})

test_that("named by and colliding columns get suffixed", {
  left <- mock_tbl(tibble::tibble(id = 1, value = 10), name = "L")
  right <- mock_tbl(tibble::tibble(ref = 1, value = 20), name = "R")

  query <- dplyr::inner_join(left, right, by = c("id" = "ref"))
  pipeline <- compile_pipeline(query)

  expect_equal(pipeline[[1]]$`$lookup`$let, list(mdb_l0 = "$id"))
  expect_equal(
    pipeline[[1]]$`$lookup`$pipeline[[1]]$`$match`$`$expr`,
    list(`$eq` = list("$ref", "$$mdb_l0"))
  )

  expect_setequal(schema_fields(query), c("id", "value.x", "value.y"))

  fm <- query$ir$field_map
  new_root <- pipeline[[3]]$`$replaceRoot`$newRoot
  expect_equal(unname(new_root[[fm[["value.x"]]]]), "$value")
  expect_equal(unname(new_root[[fm[["value.y"]]]]), "$__mdbplyr_join__.value")
})

test_that("multi-key joins emit one condition per key", {
  left <- mock_tbl(tibble::tibble(a = 1, b = 2, v = 3), name = "L")
  right <- mock_tbl(tibble::tibble(a = 1, b = 2, w = 4), name = "R")

  lookup <- compile_pipeline(dplyr::inner_join(left, right, by = c("a", "b")))[[1]]$`$lookup`

  expect_equal(lookup$let, list(mdb_l0 = "$a", mdb_l1 = "$b"))
  expect_equal(
    lookup$pipeline[[1]]$`$match`$`$expr`,
    list(`$and` = list(
      list(`$eq` = list("$a", "$$mdb_l0")),
      list(`$eq` = list("$b", "$$mdb_l1"))
    ))
  )
})

test_that("natural joins use the common columns", {
  lookup <- compile_pipeline(dplyr::inner_join(people(), worlds()))[[1]]$`$lookup`
  expect_equal(lookup$let, list(mdb_l0 = "$world_id"))
})

test_that("semi_join keeps matched left rows and drops the lookup array", {
  query <- dplyr::semi_join(people(), worlds(), by = "world_id")
  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), c("$lookup", "$match", "$project"))
  expect_equal(
    pipeline[[2]]$`$match`$`$expr`,
    list(`$gt` = list(list(`$size` = "$__mdbplyr_join_match__"), 0L))
  )
  expect_equal(pipeline[[3]]$`$project`, list(`__mdbplyr_join_match__` = 0L))
  expect_setequal(schema_fields(query), c("name", "world_id", "mass"))
})

test_that("anti_join keeps unmatched left rows", {
  pipeline <- compile_pipeline(dplyr::anti_join(people(), worlds(), by = "world_id"))

  expect_equal(
    pipeline[[2]]$`$match`$`$expr`,
    list(`$eq` = list(list(`$size` = "$__mdbplyr_join_match__"), 0L))
  )
})

test_that("unnest = FALSE keeps the match as a nested array column", {
  query <- dplyr::left_join(people(), worlds(), by = "world_id", unnest = FALSE)
  pipeline <- compile_pipeline(query)

  expect_equal(vapply(pipeline, names, character(1)), "$lookup")
  expect_equal(pipeline[[1]]$`$lookup`$as, "worlds")
  expect_setequal(schema_fields(query), c("name", "world_id", "mass", "worlds"))
})

test_that("joins reject a non-plain right-hand table", {
  transformed <- worlds() |> dplyr::filter(world_id > 0)

  expect_error(
    dplyr::inner_join(people(), transformed, by = "world_id"),
    class = "mongo_tidy_unsupported"
  )
})

test_that("joins require a known schema on the right table", {
  no_schema <- tbl_mongo(list(name = "z"), executor = function(pipeline, ...) tibble::tibble())

  expect_error(
    dplyr::inner_join(people(), no_schema, by = "world_id"),
    class = "mongo_tidy_invalid"
  )
})
