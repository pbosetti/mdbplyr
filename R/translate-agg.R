#' @keywords internal
translate_agg <- function(expr, fields = NULL, field_map = NULL) {
  env <- NULL
  if (rlang::is_quosure(expr)) {
    env <- rlang::quo_get_env(expr)
    expr <- rlang::get_expr(expr)
  }

  if (is.null(field_map) && !is.null(fields)) {
    field_map <- stats::setNames(fields, fields)
  }

  if (!rlang::is_call(expr)) {
    abort_unsupported("summarise()", expr)
  }

  fn <- rlang::call_name(expr)
  args <- rlang::call_args(expr)
  supported <- c(
    "n", "sum", "mean", "min", "max",
    "sd", "var", "first", "last", "n_distinct",
    "median", "quantile"
  )

  if (!fn %in% supported) {
    abort_unsupported(
      "summarise()", expr,
      "Supported summaries are n(), sum(), mean(), min(), max(), sd(), var(), first(), last(), n_distinct(), median(), and quantile()."
    )
  }

  if (identical(fn, "n")) {
    return(list(type = "agg", fn = "n", arg = NULL, na_rm = FALSE, prob = NULL))
  }

  env <- env %||% parent.frame()

  na_rm <- FALSE
  if (!is.null(args$na.rm)) {
    na_rm <- eval_local_literal(args$na.rm, env = env, context = "summarise()")
    if (!is.logical(na_rm) || length(na_rm) != 1L) {
      abort_unsupported("summarise()", expr, "na.rm must be a literal TRUE or FALSE.")
    }
    na_rm <- isTRUE(na_rm)
    args$na.rm <- NULL
  }

  prob <- NULL
  if (identical(fn, "quantile")) {
    prob_arg <- NULL
    if ("probs" %in% names(args)) {
      prob_arg <- args[["probs"]]
      args[["probs"]] <- NULL
    } else if (length(args) >= 2L) {
      prob_arg <- args[[2L]]
      args <- args[-2L]
    }
    if (is.null(prob_arg)) {
      abort_invalid("summarise()", "quantile() requires a single `probs` value in [0, 1].")
    }
    prob <- eval_local_literal(prob_arg, env = env, context = "summarise()")
    if (!is.numeric(prob) || length(prob) != 1L || is.na(prob) || prob < 0 || prob > 1) {
      abort_unsupported("summarise()", expr, "quantile() probs must be a single number in [0, 1].")
    }
    prob <- as.numeric(prob)
  }

  if (length(args) == 0L) {
    abort_invalid("summarise()", paste0(fn, "() requires an argument."))
  }

  if (length(args) != 1L) {
    abort_unsupported("summarise()", expr, "Only a single summary argument is supported.")
  }

  list(
    type = "agg",
    fn = fn,
    arg = translate_expr(args[[1]], context = "aggregate", env = env, field_map = field_map),
    na_rm = na_rm,
    prob = prob
  )
}

#' @keywords internal
compile_agg <- function(expr) {
  if (identical(expr$fn, "n")) {
    return(list(`$sum` = 1L))
  }

  arg <- compile_mongo_expr(expr$arg)

  switch(
    expr$fn,
    sum = list(`$sum` = arg),
    mean = list(`$avg` = arg),
    min = list(`$min` = arg),
    max = list(`$max` = arg),
    sd = list(`$stdDevSamp` = arg),
    var = list(`$stdDevSamp` = arg),
    first = list(`$first` = arg),
    last = list(`$last` = arg),
    n_distinct = list(`$addToSet` = arg),
    median = list(`$percentile` = list(input = arg, p = list(0.5), method = "approximate")),
    quantile = list(`$percentile` = list(input = arg, p = list(expr$prob), method = "approximate")),
    abort_invalid("summarise()", paste0("cannot compile aggregate ", expr$fn, "()."))
  )
}

# Post-$group projection value for an aggregate. Most accumulators map directly
# to a $group field and are simply kept (1). A few need a follow-up transform in
# the summary $project stage because MongoDB has no single accumulator for them.
#' @keywords internal
compile_summary_value <- function(name, expr) {
  ref <- field_reference(name)
  switch(
    expr$fn,
    var = list(`$pow` = list(ref, 2L)),
    n_distinct = list(`$size` = ref),
    median = list(`$arrayElemAt` = list(ref, 0L)),
    quantile = list(`$arrayElemAt` = list(ref, 0L)),
    1L
  )
}
