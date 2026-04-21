#' @keywords internal
translate_agg <- function(expr, fields = NULL) {
  env <- NULL
  if (rlang::is_quosure(expr)) {
    env <- rlang::quo_get_env(expr)
    expr <- rlang::get_expr(expr)
  }

  if (!rlang::is_call(expr)) {
    abort_unsupported("summarise()", expr)
  }

  fn <- rlang::call_name(expr)
  args <- rlang::call_args(expr)
  supported <- c("n", "sum", "mean", "min", "max")

  if (!fn %in% supported) {
    abort_unsupported("summarise()", expr, "Supported summaries are n(), sum(), mean(), min(), and max().")
  }

  if (identical(fn, "n")) {
    return(list(type = "agg", fn = "n", arg = NULL, na_rm = FALSE))
  }

  if (length(args) == 0) {
    abort_invalid("summarise()", paste0(fn, "() requires an argument."))
  }

  na_rm <- FALSE
  if (!is.null(args$na.rm)) {
    na_rm <- eval_local_literal(args$na.rm, env = env %||% parent.frame(), context = "summarise()")
    if (!is.logical(na_rm) || length(na_rm) != 1L) {
      abort_unsupported("summarise()", expr, "na.rm must be a literal TRUE or FALSE.")
    }
    na_rm <- isTRUE(na_rm)
    args$na.rm <- NULL
  }

  if (length(args) != 1) {
    abort_unsupported("summarise()", expr, "Only a single summary argument is supported.")
  }

  list(
    type = "agg",
    fn = fn,
    arg = translate_expr(args[[1]], context = "aggregate", env = env, fields = fields),
    na_rm = na_rm
  )
}

#' @keywords internal
compile_agg <- function(expr) {
  if (identical(expr$fn, "n")) {
    return(list(`$sum` = 1L))
  }

  operator <- switch(
    expr$fn,
    sum = "$sum",
    mean = "$avg",
    min = "$min",
    max = "$max"
  )

  stats::setNames(list(compile_mongo_expr(expr$arg)), operator)
}
