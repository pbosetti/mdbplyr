# Window functions compile to a MongoDB $setWindowFields stage (server 5.0+).
# Each window assignment becomes its own stage so a single mutate() can mix
# windows with different sort keys. partitionBy comes from group_by(); sortBy
# comes either from an explicit ranking column or from a preceding arrange().

#' @keywords internal
window_op_map <- function() {
  list(
    rank_ops = c(rank = "$rank", min_rank = "$rank", dense_rank = "$denseRank"),
    cum_ops = c(cumsum = "$sum", cummean = "$avg", cummax = "$max", cummin = "$min"),
    shift_ops = c("lag", "lead")
  )
}

#' @keywords internal
is_window_expr <- function(expr) {
  if (rlang::is_quosure(expr)) {
    expr <- rlang::get_expr(expr)
  }
  if (!rlang::is_call(expr)) {
    return(FALSE)
  }
  fn <- tryCatch(rlang::call_name(expr), error = function(...) NULL)
  if (is.null(fn)) {
    return(FALSE)
  }
  ops <- window_op_map()
  fn %in% c(names(ops$rank_ops), names(ops$cum_ops), ops$shift_ops)
}

#' @keywords internal
empty_mongo_object <- function() {
  structure(list(), names = character())
}

#' @keywords internal
window_step <- function(field, output, partition_by, sort_by) {
  list(
    type = "window",
    field = field,
    output = output,
    partition_by = partition_by,
    sort_by = sort_by
  )
}

#' @keywords internal
require_window_sort <- function(pipeline_sort, fn, context) {
  if (is.null(pipeline_sort) || !length(pipeline_sort)) {
    abort_invalid(context, paste0(fn, "() requires a preceding arrange() to define row order."))
  }
}

#' @keywords internal
parse_window_order_column <- function(arg, field_map, context) {
  dir <- 1L
  if (rlang::is_call(arg, "desc")) {
    dir <- -1L
    arg <- rlang::call_args(arg)[[1]]
  }
  if (!rlang::is_symbol(arg)) {
    abort_unsupported(context, arg, "ranking window functions require a bare field name (optionally wrapped in desc()).")
  }
  stats::setNames(list(dir), resolve_field_sources(rlang::as_string(arg), field_map))
}

#' @keywords internal
build_window_step <- function(quo, field, field_map, partition_by, pipeline_sort, context) {
  expr <- rlang::quo_get_expr(quo)
  env <- rlang::quo_get_env(quo)
  fn <- rlang::call_name(expr)
  args <- rlang::call_args(expr)
  ops <- window_op_map()

  if (fn %in% names(ops$rank_ops)) {
    if (length(args) != 1L) {
      abort_invalid(context, paste0(fn, "() requires exactly one column."))
    }
    sort_by <- parse_window_order_column(args[[1]], field_map, context)
    output <- stats::setNames(list(empty_mongo_object()), ops$rank_ops[[fn]])
    return(window_step(field, output, partition_by, sort_by))
  }

  if (fn %in% names(ops$cum_ops)) {
    if (length(args) != 1L) {
      abort_invalid(context, paste0(fn, "() requires exactly one argument."))
    }
    require_window_sort(pipeline_sort, fn, context)
    arg <- compile_mongo_expr(translate_expr(args[[1]], context = context, env = env, field_map = field_map))
    output <- stats::setNames(
      list(arg, list(documents = list("unbounded", "current"))),
      c(ops$cum_ops[[fn]], "window")
    )
    return(window_step(field, output, partition_by, pipeline_sort))
  }

  build_shift_step(expr, fn, field, field_map, partition_by, pipeline_sort, env, context)
}

#' @keywords internal
build_shift_step <- function(expr, fn, field, field_map, partition_by, pipeline_sort, env, context) {
  require_window_sort(pipeline_sort, fn, context)
  margs <- rlang::call_args(rlang::call_match(expr, get(fn, envir = asNamespace("dplyr"))))

  if (!is.null(margs$order_by)) {
    abort_unsupported(context, expr, "order_by is not supported; use a preceding arrange() instead.")
  }
  if (is.null(margs$x)) {
    abort_invalid(context, paste0(fn, "() requires a column."))
  }
  arg <- compile_mongo_expr(translate_expr(margs$x, context = context, env = env, field_map = field_map))

  n_val <- 1L
  if (!is.null(margs$n)) {
    n_val <- eval_local_literal(margs$n, env = env, context = context)
    if (!is.numeric(n_val) || length(n_val) != 1L || is.na(n_val) || n_val < 0 || n_val != trunc(n_val)) {
      abort_unsupported(context, expr, paste0(fn, "() n must be a single non-negative integer."))
    }
    n_val <- as.integer(n_val)
  }
  by <- if (identical(fn, "lag")) -n_val else n_val

  shift <- list(output = arg, by = by)
  if (!is.null(margs$default)) {
    default_val <- eval_local_literal(margs$default, env = env, context = context)
    if (!(length(default_val) == 1L && is.na(default_val))) {
      shift$default <- as_mongo_literal(default_val)
    }
  }

  window_step(field, list(`$shift` = shift), partition_by, pipeline_sort)
}

#' @keywords internal
compile_partition_by <- function(groups, group_defs) {
  if (!length(groups)) {
    return(NULL)
  }
  keys <- lapply(groups, function(group) compile_group_key(group_defs[[group]]))
  if (length(groups) == 1L) {
    return(keys[[1]])
  }
  stats::setNames(keys, groups)
}

#' @keywords internal
compile_window_stage <- function(step) {
  swf <- list()
  if (!is.null(step$partition_by)) {
    swf$partitionBy <- step$partition_by
  }
  if (!is.null(step$sort_by) && length(step$sort_by)) {
    swf$sortBy <- lapply(step$sort_by, as.integer)
  }
  swf$output <- stats::setNames(list(step$output), step$field)
  list(list(`$setWindowFields` = swf))
}
