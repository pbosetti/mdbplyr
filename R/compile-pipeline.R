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

  if (!is.null(ir$join)) {
    join_stages <- compile_join_stages(ir$join)
    stages <- c(stages, join_stages)
  }

  if (length(ir$order) > 0) {
    stages[[length(stages) + 1]] <- list(`$sort` = lapply(ir$order, as.integer))
  }

  if (!is.null(ir$limit)) {
    stages[[length(stages) + 1]] <- list(`$limit` = as.integer(ir$limit))
  }

  if (length(ir$manual_stages) > 0) {
    stages <- c(stages, ir$manual_stages)
  }

  stages
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

#' @keywords internal
compile_join_stages <- function(join) {
  JOINED <- "__mdbplyr_joined__"

  # ---- build $lookup with embedded pipeline ---------------------------------
  # Use the correlated-pipeline form of $lookup (MongoDB 3.6+) so that:
  #  * multiple join keys are supported, and
  #  * any pipeline already on y is embedded inside the lookup.
  let_vars <- stats::setNames(
    lapply(join$local_keys, field_reference),
    paste0("_mdbplyr_", seq_along(join$local_keys))
  )

  match_conditions <- lapply(seq_along(join$local_keys), function(i) {
    list(`$eq` = list(
      field_reference(join$foreign_keys[[i]]),
      paste0("$$_mdbplyr_", i)
    ))
  })

  match_expr <- if (length(match_conditions) == 1L) {
    match_conditions[[1L]]
  } else {
    list(`$and` = match_conditions)
  }

  # Correlated $match is first; any stages already compiled from y follow.
  embedded_pipeline <- c(
    list(list(`$match` = list(`$expr` = match_expr))),
    join$y_pipeline
  )

  stages <- list(
    list(`$lookup` = list(
      from     = join$from,
      let      = let_vars,
      pipeline = embedded_pipeline,
      as       = JOINED
    ))
  )

  # ---- join-type-specific stages --------------------------------------------
  if (join$type == "semi") {
    # Keep only rows that have at least one match.
    stages[[2L]] <- list(`$match` = stats::setNames(
      list(list(`$ne` = list())), JOINED
    ))
    stages[[3L]] <- list(`$project` = stats::setNames(list(0L), JOINED))

  } else if (join$type == "anti") {
    # Keep only rows with no match.
    stages[[2L]] <- list(`$match` = stats::setNames(
      list(list(`$size` = 0L)), JOINED
    ))
    stages[[3L]] <- list(`$project` = stats::setNames(list(0L), JOINED))

  } else {
    # inner / left: unwind, merge, remove helper field.
    unwind_spec <- if (join$type == "inner") {
      paste0("$", JOINED)
    } else {
      list(path = paste0("$", JOINED), preserveNullAndEmptyArrays = TRUE)
    }
    stages[[2L]] <- list(`$unwind` = unwind_spec)

    # For left joins, guard against a NULL __joined__ after unwind.
    joined_ref <- if (join$type == "left") {
      list(`$ifNull` = list(field_reference(JOINED), list()))
    } else {
      field_reference(JOINED)
    }

    # Merge y's fields under x's fields (x wins on conflict).
    stages[[3L]] <- list(`$replaceRoot` = list(
      newRoot = list(`$mergeObjects` = list(joined_ref, "$$ROOT"))
    ))

    # Remove the helper array and foreign keys that differ from local keys.
    remove_fields <- unique(c(JOINED, setdiff(join$foreign_keys, join$local_keys)))
    stages[[4L]] <- list(`$project` = stats::setNames(
      as.list(rep(0L, length(remove_fields))), remove_fields
    ))
  }

  stages
}
