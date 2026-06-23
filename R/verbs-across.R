#' @keywords internal
is_across_call <- function(expr) {
  rlang::is_call(expr) &&
    identical(tryCatch(rlang::call_name(expr), error = function(...) NULL), "across")
}

# Desugar any across() quosures into a flat, named list of ordinary quosures so
# the existing mutate()/summarise() machinery can translate them unchanged. This
# is a purely syntactic expansion: across(.cols, .fns) becomes one named
# assignment per (column, function) pair.
#' @keywords internal
expand_across_quos <- function(quos, fields, context) {
  if (!length(quos)) {
    return(quos)
  }
  if (!any(vapply(quos, function(quo) is_across_call(rlang::quo_get_expr(quo)), logical(1)))) {
    return(quos)
  }

  names_in <- rlang::names2(quos)
  out <- list()
  for (i in seq_along(quos)) {
    quo <- quos[[i]]
    if (is_across_call(rlang::quo_get_expr(quo))) {
      out <- c(out, expand_one_across(quo, fields, context))
    } else {
      out <- c(out, stats::setNames(list(quo), names_in[[i]]))
    }
  }
  out
}

#' @keywords internal
expand_one_across <- function(quo, fields, context) {
  if (!length(fields)) {
    abort_invalid(context, "across() requires a known schema. Supply schema to tbl_mongo() or call infer_schema().")
  }

  expr <- rlang::quo_get_expr(quo)
  env <- rlang::quo_get_env(quo)
  margs <- rlang::call_args(rlang::call_match(expr, dplyr::across))

  cols_expr <- margs[[".cols"]]
  if (is.null(cols_expr)) {
    abort_invalid(context, "across() requires a column selection.")
  }
  fns_expr <- margs[[".fns"]]
  if (is.null(fns_expr)) {
    abort_invalid(context, "across() requires a function or list of functions.")
  }

  names_glue <- NULL
  if (!is.null(margs[[".names"]])) {
    names_glue <- eval_local_literal(margs[[".names"]], env = env, context = context)
    if (!is.character(names_glue) || length(names_glue) != 1L) {
      abort_invalid(context, "across(.names = ) must be a single string.")
    }
  }

  forwarded <- margs[setdiff(names(margs), c(".cols", ".fns", ".names", ".unpack"))]

  if (expr_uses_where(cols_expr)) {
    abort_unsupported(context, cols_expr, "where() selections are not supported because column types are unknown without reading data.")
  }
  proxy <- tibble::as_tibble(
    stats::setNames(rep(list(logical()), length(fields)), fields),
    .name_repair = "minimal"
  )
  loc <- tidyselect::eval_select(rlang::new_quosure(cols_expr, env), data = proxy)
  col_sources <- fields[loc]
  col_labels <- names(loc)

  fn_entries <- across_fn_entries(fns_expr, context)
  if (is.null(names_glue)) {
    names_glue <- if (isTRUE(attr(fn_entries, "multi"))) "{.col}_{.fn}" else "{.col}"
  }

  out <- list()
  for (j in seq_along(col_sources)) {
    col_sym <- rlang::sym(col_sources[[j]])
    for (entry in fn_entries) {
      out_name <- across_make_name(names_glue, col_labels[[j]], entry$label)
      out[[out_name]] <- rlang::new_quosure(across_apply(entry, col_sym, forwarded), env)
    }
  }
  out
}

#' @keywords internal
across_fn_entries <- function(fns_expr, context) {
  if (rlang::is_call(fns_expr, "list")) {
    items <- rlang::call_args(fns_expr)
    labels <- rlang::names2(items)
    entries <- lapply(seq_along(items), function(k) {
      label <- labels[[k]]
      if (!nzchar(label)) label <- as.character(k)
      across_make_entry(items[[k]], label, context)
    })
    attr(entries, "multi") <- TRUE
    return(entries)
  }

  entries <- list(across_make_entry(fns_expr, "1", context))
  attr(entries, "multi") <- FALSE
  entries
}

#' @keywords internal
across_make_entry <- function(fn_expr, label, context) {
  if (rlang::is_formula(fn_expr)) {
    return(list(label = label, kind = "formula", rhs = rlang::f_rhs(fn_expr)))
  }
  if (rlang::is_symbol(fn_expr) || rlang::is_call(fn_expr, "::")) {
    return(list(label = label, kind = "call", fn = fn_expr))
  }
  abort_unsupported(
    context, fn_expr,
    "across() functions must be a bare function name or a ~ lambda; anonymous functions and function variables are not supported."
  )
}

#' @keywords internal
across_apply <- function(entry, col_sym, forwarded) {
  if (identical(entry$kind, "formula")) {
    return(across_substitute_dot(entry$rhs, col_sym))
  }
  as.call(c(list(entry$fn, col_sym), forwarded))
}

#' @keywords internal
across_substitute_dot <- function(expr, col_sym) {
  if (rlang::is_symbol(expr)) {
    if (identical(expr, rlang::sym(".x")) || identical(expr, rlang::sym("."))) {
      return(col_sym)
    }
    return(expr)
  }
  if (rlang::is_call(expr)) {
    return(as.call(lapply(as.list(expr), across_substitute_dot, col_sym = col_sym)))
  }
  expr
}

#' @keywords internal
across_make_name <- function(glue, col, fn) {
  out <- gsub("{.col}", col, glue, fixed = TRUE)
  gsub("{.fn}", fn, out, fixed = TRUE)
}
