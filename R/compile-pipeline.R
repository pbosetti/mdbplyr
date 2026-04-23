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

  if (length(ir$ops) > 0) {
    for (op in ir$ops) {
      stages <- c(stages, compile_ir_op(op))
    }
    if (length(ir$manual_stages) > 0) {
      stages <- c(stages, ir$manual_stages)
    }
    return(stages)
  }

  if (length(ir$filters) > 0) {
    predicate <- if (length(ir$filters) == 1) {
      compile_mongo_expr(ir$filters[[1]])
    } else {
      list(`$and` = lapply(ir$filters, compile_mongo_expr))
    }
    stages[[length(stages) + 1]] <- list(`$match` = list(`$expr` = predicate))
  }

  if (length(ir$computed) > 0) {
    stages[[length(stages) + 1]] <- compile_add_fields_stage(ir$computed)
  }

  if (!is.null(ir$projection)) {
    stages[[length(stages) + 1]] <- list(`$project` = compile_projection_stage(ir$projection))
  }

  if (length(ir$summaries) > 0) {
    stages[[length(stages) + 1]] <- list(`$group` = compile_group_stage(ir))
    stages[[length(stages) + 1]] <- list(`$project` = compile_summary_projection(ir))
  }

  if (length(ir$row_ops) > 0) {
    for (row_op in ir$row_ops) {
      stages <- c(stages, compile_row_op_stages(row_op))
    }
  } else {
    if (length(ir$order) > 0) {
      stages[[length(stages) + 1]] <- list(`$sort` = lapply(ir$order, as.integer))
    }

    if (length(ir$slices) > 0) {
      for (slice in ir$slices) {
        stages <- c(stages, compile_slice_stages(slice))
      }
    }
  }

  if (length(ir$manual_stages) > 0) {
    stages <- c(stages, ir$manual_stages)
  }

  stages
}

#' @keywords internal
compile_ir_op <- function(op) {
  switch(
    op$type,
    filter = list(list(`$match` = list(`$expr` = compile_filter_expr(op$predicates)))),
    mutate = compile_mutate_stages(op),
    project = list(list(`$project` = compile_projection_stage(op$projection))),
    summarise = list(
      list(`$group` = compile_group_stage(op)),
      list(`$project` = compile_summary_projection(op))
    ),
    sort = list(list(`$sort` = lapply(op$order, as.integer))),
    slice = compile_slice_stages(op$slice),
    unwind = list(list(`$unwind` = c(
      list(path = field_reference(op$field_source)),
      if (isTRUE(op$preserve_empty)) list(preserveNullAndEmptyArrays = TRUE) else list()
    ))),
    abort_invalid("compile_pipeline()", paste("cannot compile op", op$type))
  )
}

#' @keywords internal
compile_add_fields_stage <- function(computed) {
  list(`$addFields` = lapply(computed, compile_mongo_expr))
}

#' @keywords internal
compile_mutate_stages <- function(op) {
  if (!is.null(op$steps)) {
    stages <- list()

    for (step in op$steps) {
      stages <- c(stages, switch(
        step$type,
        computed = list(compile_add_fields_stage(stats::setNames(list(step$expr), step$field))),
        sequence = compile_sequence_stages(step$fields, step$groups),
        abort_invalid("compile_pipeline()", paste("cannot compile mutate step", step$type))
      ))
    }

    return(stages)
  }

  c(
    if (length(op$computed) > 0) {
      list(compile_add_fields_stage(op$computed))
    } else {
      list()
    },
    if (length(op$sequence_fields) > 0) {
      compile_sequence_stages(op$sequence_fields, op$groups)
    } else {
      list()
    }
  )
}

#' @keywords internal
compile_filter_expr <- function(predicates) {
  if (length(predicates) == 1L) {
    return(compile_mongo_expr(predicates[[1]]))
  }

  list(`$and` = lapply(predicates, compile_mongo_expr))
}

#' @keywords internal
compile_row_op_stages <- function(row_op) {
  switch(
    row_op$type,
    sort = list(list(`$sort` = lapply(row_op$order, as.integer))),
    slice = compile_slice_stages(row_op$slice),
    sequence = compile_sequence_stages(row_op$fields, row_op$groups),
    abort_invalid("compile_pipeline()", paste("cannot compile row op", row_op$type))
  )
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
compile_sequence_stages <- function(fields, groups) {
  docs_field <- "__mdbplyr_seq_docs__"
  idx_field <- "__mdbplyr_seq_idx__"
  global_idx_field <- "__mdbplyr_seq_global_idx__"

  if (length(groups) == 0L) {
    return(list(
      list(`$group` = stats::setNames(
        list(NULL, list(`$push` = "$$ROOT")),
        c("_id", docs_field)
      )),
      list(`$unwind` = list(path = paste0("$", docs_field), includeArrayIndex = idx_field)),
      list(`$replaceRoot` = list(
        newRoot = list(`$mergeObjects` = list(
          paste0("$", docs_field),
          compile_sequence_field_object(fields, idx_field)
        ))
      ))
    ))
  }

  list(
    list(`$group` = stats::setNames(
      list(NULL, list(`$push` = "$$ROOT")),
      c("_id", docs_field)
    )),
    list(`$unwind` = list(path = paste0("$", docs_field), includeArrayIndex = idx_field)),
    list(`$replaceRoot` = list(
      newRoot = list(`$mergeObjects` = list(
        paste0("$", docs_field),
        stats::setNames(list(field_reference(idx_field)), global_idx_field)
      ))
    )),
    list(`$group` = c(
      list(`_id` = stats::setNames(lapply(groups, field_reference), groups)),
      stats::setNames(list(list(`$push` = "$$ROOT")), docs_field)
    )),
    list(`$unwind` = list(path = paste0("$", docs_field), includeArrayIndex = idx_field)),
    list(`$replaceRoot` = list(
      newRoot = list(`$mergeObjects` = list(
        paste0("$", docs_field),
        compile_sequence_field_object(fields, idx_field)
      ))
    )),
    list(`$sort` = stats::setNames(list(1L), global_idx_field)),
    list(`$project` = stats::setNames(list(0L), global_idx_field))
  )
}

#' @keywords internal
compile_sequence_field_object <- function(fields, idx_field) {
  stats::setNames(
    rep(list(list(`$add` = list(field_reference(idx_field), 1L))), length(fields)),
    fields
  )
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
  groups <- ir$groups %||% character()
  group_sources <- ir$group_sources %||% stats::setNames(groups, groups)

  id_stage <- if (length(groups) == 0) {
    NULL
  } else {
    stats::setNames(lapply(unname(group_sources), field_reference), names(group_sources))
  }

  summaries <- lapply(ir$summaries, compile_agg)
  c(list(`_id` = id_stage), summaries)
}

#' @keywords internal
compile_summary_projection <- function(ir) {
  stage <- list(`_id` = 0L)
  groups <- ir$groups %||% character()
  if (length(groups) > 0) {
    for (group in groups) {
      stage[[group]] <- field_reference(paste0("_id.", group))
    }
  }
  for (name in names(ir$summaries)) {
    stage[[name]] <- 1L
  }
  stage
}
