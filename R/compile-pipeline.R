#' Compile a lazy Mongo query into an aggregation pipeline
#'
#' @param x A `tbl_mongo` object.
#'
#' @return A list of MongoDB aggregation stages.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' query <- dplyr::filter(tbl, amount > 0)
#' compile_pipeline(query)
#' @export
compile_pipeline <- function(x) {
  if (!inherits(x, "tbl_mongo")) {
    abort_invalid("compile_pipeline()", "requires a tbl_mongo object.")
  }

  ir <- x$ir
  stages <- list()

  if (length(ir$filters) > 0) {
    predicate <- if (length(ir$filters) == 1) {
      compile_mongo_expr(ir$filters[[1]])
    } else {
      list(`$and` = lapply(ir$filters, compile_mongo_expr))
    }
    stages[[length(stages) + 1]] <- list(`$match` = list(`$expr` = predicate))
  }

  if (length(ir$computed) > 0) {
    stages[[length(stages) + 1]] <- list(`$addFields` = lapply(ir$computed, compile_mongo_expr))
  }

  if (!is.null(ir$projection)) {
    stages[[length(stages) + 1]] <- list(`$project` = compile_projection_stage(ir$projection))
  }

  if (length(ir$summaries) > 0) {
    stages[[length(stages) + 1]] <- list(`$group` = compile_group_stage(ir))
    stages[[length(stages) + 1]] <- list(`$project` = compile_summary_projection(ir))
  }

  if (length(ir$order) > 0) {
    stages[[length(stages) + 1]] <- list(`$sort` = lapply(ir$order, as.integer))
  }

  if (length(ir$slices) > 0) {
    for (slice in ir$slices) {
      stages <- c(stages, compile_slice_stages(slice))
    }
  }

  if (length(ir$manual_stages) > 0) {
    stages <- c(stages, ir$manual_stages)
  }

  stages
}

#' @keywords internal
compile_slice_stages <- function(slice) {
  if (identical(slice$verb, "head") && slice$n >= 0L) {
    return(list(list(`$limit` = as.integer(slice$n))))
  }

  docs_field <- "__mdbplyr_slice_docs__"
  slice_ref <- field_reference(docs_field)

  list(
    list(`$group` = stats::setNames(
      list(NULL, list(`$push` = "$$ROOT")),
      c("_id", docs_field)
    )),
    list(`$project` = stats::setNames(
      list(0L, compile_slice_expr(slice, slice_ref)),
      c("_id", docs_field)
    )),
    list(`$unwind` = paste0("$", docs_field)),
    list(`$replaceRoot` = list(newRoot = paste0("$", docs_field)))
  )
}

#' @keywords internal
compile_slice_expr <- function(slice, docs_ref) {
  n <- as.integer(slice$n)

  if (identical(slice$verb, "tail") && n >= 0L) {
    return(list(`$slice` = list(docs_ref, -n)))
  }

  drop_n <- abs(n)
  remaining <- list(`$max` = list(
    0L,
    list(`$subtract` = list(list(`$size` = docs_ref), drop_n))
  ))

  if (identical(slice$verb, "head")) {
    return(list(`$slice` = list(docs_ref, remaining)))
  }

  list(`$slice` = list(docs_ref, drop_n, remaining))
}

#' @keywords internal
compile_projection_stage <- function(projection) {
  stage <- lapply(names(projection), function(name) {
    source <- unname(projection[[name]])
    if (identical(name, source)) 1L else field_reference(source)
  })
  names(stage) <- names(projection)
  if (!"_id" %in% names(stage)) {
    stage[["_id"]] <- 0L
  }
  stage
}

#' @keywords internal
compile_group_stage <- function(ir) {
  id_stage <- if (length(ir$groups) == 0) {
    NULL
  } else {
    stats::setNames(lapply(ir$groups, field_reference), ir$groups)
  }

  summaries <- lapply(ir$summaries, compile_agg)
  c(list(`_id` = id_stage), summaries)
}

#' @keywords internal
compile_summary_projection <- function(ir) {
  stage <- list(`_id` = 0L)
  if (length(ir$groups) > 0) {
    for (group in ir$groups) {
      stage[[group]] <- field_reference(paste0("_id.", group))
    }
  }
  for (name in names(ir$summaries)) {
    stage[[name]] <- 1L
  }
  stage
}
