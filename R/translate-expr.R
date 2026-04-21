#' @keywords internal
make_call_expr <- function(fn, args) {
  list(type = "call", fn = fn, args = args)
}

#' @keywords internal
is_local_literal_value <- function(x) {
  is.null(x) || (is.atomic(x) && is.null(dim(x)))
}

#' @keywords internal
eval_local_literal <- function(expr, env, context) {
  label <- expr_text(expr)

  value <- tryCatch(
    rlang::eval_tidy(rlang::new_quosure(expr, env = env)),
    error = function(cnd) {
      abort_invalid(context, paste0("cannot evaluate local expression `", label, "`: ", cnd$message))
    }
  )

  if (!is_local_literal_value(value)) {
    abort_invalid(context, paste0("local expression `", label, "` must evaluate to an atomic or NULL literal."))
  }

  value
}

#' @keywords internal
translate_local_literal <- function(expr, env, context) {
  list(type = "literal", value = eval_local_literal(expr, env, context))
}

#' @keywords internal
translate_local_scalar <- function(expr, env, context, fn, validator, details) {
  value <- eval_local_literal(expr, env, context)
  if (!validator(value)) {
    abort_unsupported(context, expr, paste0(fn, "() ", details))
  }
  value
}

#' @keywords internal
pronoun_reference_name <- function(expr, pronoun, context) {
  if (!rlang::is_call(expr, "$")) {
    abort_unsupported(context, expr)
  }

  args <- rlang::call_args(expr)
  if (length(args) != 2L || !rlang::is_symbol(args[[1]], pronoun) || !rlang::is_symbol(args[[2]])) {
    abort_unsupported(context, expr, "Only `.data$col` and `.env$x` references are supported.")
  }

  rlang::as_string(args[[2]])
}

#' @keywords internal
expr_has_field_reference <- function(expr, fields = character()) {
  if (rlang::is_quosure(expr)) {
    expr <- rlang::get_expr(expr)
  }

  if (rlang::is_symbol(expr)) {
    return(is.null(fields) || rlang::as_string(expr) %in% fields)
  }

  if (!rlang::is_call(expr)) {
    return(FALSE)
  }

  if (rlang::is_call(expr, "$")) {
    lhs <- rlang::call_args(expr)[[1]]
    if (rlang::is_symbol(lhs, ".data")) {
      return(TRUE)
    }
    if (rlang::is_symbol(lhs, ".env")) {
      return(FALSE)
    }
  }

  any(vapply(rlang::call_args(expr), expr_has_field_reference, logical(1), fields = fields))
}

#' @keywords internal
translate_literal_vector <- function(args, context, expr) {
  if (!length(args) || !all(vapply(args, is_scalar_literal, logical(1)))) {
    abort_unsupported(context, expr, "c() only supports literal vectors.")
  }

  list(
    type = "literal",
    value = rlang::eval_bare(rlang::call2("c", !!!args), env = baseenv())
  )
}

#' @keywords internal
translate_log_expr <- function(args, context, expr, env, fields) {
  if (!length(args) || length(args) > 2) {
    abort_unsupported(context, expr, "log() supports one argument or `log(x, base)`.")
  }

  if (length(args) == 1) {
    return(make_call_expr("ln", lapply(args, function(arg) translate_expr(arg, context = context, env = env, fields = fields))))
  }

  make_call_expr("log", lapply(args[1:2], function(arg) translate_expr(arg, context = context, env = env, fields = fields)))
}

#' @keywords internal
translate_nchar_expr <- function(args, context, expr, env, fields) {
  if (length(args) != 1) {
    abort_unsupported(context, expr, "nchar() currently supports a single argument.")
  }

  make_call_expr("strLenCP", lapply(args, function(arg) translate_expr(arg, context = context, env = env, fields = fields)))
}

#' @keywords internal
translate_paste_expr <- function(args, context, expr, env, fields, default_sep = "") {
  arg_names <- rlang::names2(args)
  pieces <- args[!nzchar(arg_names)]
  named_args <- args[nzchar(arg_names)]

  if (!length(pieces)) {
    abort_invalid("paste()", "requires at least one value.")
  }

  sep <- default_sep
  if ("sep" %in% names(named_args)) {
    sep <- translate_local_scalar(
      named_args$sep,
      env = env,
      context = context,
      fn = "paste",
      validator = function(value) is.character(value) && length(value) == 1L,
      details = "only supports a literal character separator."
    )
  }

  if ("collapse" %in% names(named_args) && !is.null(named_args$collapse)) {
    abort_unsupported(context, expr, "paste() does not support `collapse`.")
  }

  translated <- lapply(pieces, function(arg) translate_expr(arg, context = context, env = env, fields = fields))
  concat_args <- list()
  for (i in seq_along(translated)) {
    concat_args[[length(concat_args) + 1]] <- translated[[i]]
    if (i < length(translated) && nzchar(sep)) {
      concat_args[[length(concat_args) + 1]] <- list(type = "literal", value = sep)
    }
  }

  make_call_expr("concat", concat_args)
}

#' @keywords internal
translate_substr_expr <- function(args, context, expr, env, fields) {
  arg_names <- rlang::names2(args)
  source_arg <- if ("x" %in% names(args)) args$x else if ("text" %in% names(args)) args$text else args[[1]]
  start_arg <- if ("start" %in% names(args)) args$start else if ("first" %in% names(args)) args$first else args[[2]]
  stop_arg <- if ("stop" %in% names(args)) args$stop else if ("last" %in% names(args)) args$last else args[[3]]

  if (length(args) < 3 || is.null(source_arg) || is.null(start_arg) || is.null(stop_arg)) {
    abort_unsupported(context, expr, "substr()/substring() require source, start, and stop.")
  }

  start_expr <- translate_expr(start_arg, context = context, env = env, fields = fields)
  stop_expr <- translate_expr(stop_arg, context = context, env = env, fields = fields)

  zero_based_start <- make_call_expr("subtract", list(
    start_expr,
    list(type = "literal", value = 1)
  ))
  width_expr <- make_call_expr("add", list(
    make_call_expr("subtract", list(stop_expr, start_expr)),
    list(type = "literal", value = 1)
  ))

  make_call_expr("substrCP", list(
    translate_expr(source_arg, context = context, env = env, fields = fields),
    zero_based_start,
    width_expr
  ))
}

#' @keywords internal
translate_expr <- function(expr, context = "scalar", env = NULL, fields = NULL) {
  if (rlang::is_quosure(expr)) {
    env <- rlang::quo_get_env(expr)
    expr <- rlang::get_expr(expr)
  }

  env <- env %||% parent.frame()

  if (rlang::is_symbol(expr)) {
    name <- rlang::as_string(expr)
    if (is.null(fields) || name %in% fields) {
      return(list(type = "field", name = name))
    }
    return(translate_local_literal(expr, env = env, context = context))
  }

  if (is_scalar_literal(expr)) {
    return(list(type = "literal", value = expr))
  }

  if (!rlang::is_call(expr)) {
    abort_unsupported(context, expr)
  }

  fn <- rlang::call_name(expr)
  args <- rlang::call_args(expr)

  if (rlang::is_call(expr, "$")) {
    lhs <- args[[1]]
    if (rlang::is_symbol(lhs, ".data")) {
      return(list(type = "field", name = pronoun_reference_name(expr, ".data", context)))
    }
    if (rlang::is_symbol(lhs, ".env")) {
      return(list(
        type = "literal",
        value = eval_local_literal(rlang::sym(pronoun_reference_name(expr, ".env", context)), env = env, context = context)
      ))
    }
  }

  if (identical(fn, "(")) {
    return(translate_expr(args[[1]], context = context, env = env, fields = fields))
  }

  if (!is.null(fields) && !expr_has_field_reference(expr, fields = fields)) {
    return(translate_local_literal(expr, env = env, context = context))
  }

  comparison_map <- c(`==` = "eq", `!=` = "ne", `>` = "gt", `>=` = "gte", `<` = "lt", `<=` = "lte")
  boolean_map <- c(`&` = "and", `|` = "or")
  arithmetic_map <- c(`+` = "add", `-` = "subtract", `*` = "multiply", `/` = "divide", `%%` = "mod", `^` = "pow")
  scalar_map <- c(
    abs = "abs",
    sqrt = "sqrt",
    exp = "exp",
    floor = "floor",
    ceiling = "ceil",
    trunc = "trunc",
    log10 = "log10",
    sin = "sin",
    cos = "cos",
    tan = "tan",
    asin = "asin",
    acos = "acos",
    atan = "atan",
    sinh = "sinh",
    cosh = "cosh",
    tanh = "tanh",
    asinh = "asinh",
    acosh = "acosh",
    atanh = "atanh",
    tolower = "toLower",
    toupper = "toUpper"
  )
  variadic_map <- c(pmin = "min", pmax = "max", atan2 = "atan2")

  if (identical(fn, "c")) {
    return(translate_literal_vector(args, context, expr))
  }

  if (fn %in% names(comparison_map)) {
    return(list(
      type = "comparison",
      fn = comparison_map[[fn]],
      args = lapply(args, function(arg) translate_expr(arg, context = "predicate", env = env, fields = fields))
    ))
  }

  if (fn %in% names(boolean_map)) {
    return(list(
      type = "boolean",
      fn = boolean_map[[fn]],
      args = lapply(args, function(arg) translate_expr(arg, context = "predicate", env = env, fields = fields))
    ))
  }

  if (identical(fn, "!")) {
    return(list(type = "not", arg = translate_expr(args[[1]], context = "predicate", env = env, fields = fields)))
  }

  if (fn %in% names(arithmetic_map)) {
    if (length(args) == 1 && identical(fn, "+")) {
      return(translate_expr(args[[1]], context = context, env = env, fields = fields))
    }

    if (length(args) == 1 && identical(fn, "-")) {
      return(make_call_expr("multiply", list(
        list(type = "literal", value = -1),
        translate_expr(args[[1]], context = context, env = env, fields = fields)
      )))
    }

    return(make_call_expr(arithmetic_map[[fn]], lapply(args, function(arg) translate_expr(arg, context = context, env = env, fields = fields))))
  }

  if (fn %in% names(scalar_map)) {
    return(make_call_expr(scalar_map[[fn]], lapply(args, function(arg) translate_expr(arg, context = context, env = env, fields = fields))))
  }

  if (identical(fn, "log")) {
    return(translate_log_expr(args, context, expr, env = env, fields = fields))
  }

  if (fn %in% names(variadic_map)) {
    if (!length(args)) {
      abort_invalid(paste0(fn, "()"), "requires at least one argument.")
    }
    return(make_call_expr(variadic_map[[fn]], lapply(args, function(arg) translate_expr(arg, context = context, env = env, fields = fields))))
  }

  if (identical(fn, "%in%")) {
    if (length(args) != 2) {
      abort_invalid("%in%", "requires left and right operands.")
    }
    return(make_call_expr("in", lapply(args, function(arg) translate_expr(arg, context = context, env = env, fields = fields))))
  }

  if (fn %in% c("paste", "paste0")) {
    return(translate_paste_expr(
      args,
      context,
      expr,
      env = env,
      fields = fields,
      default_sep = if (identical(fn, "paste")) " " else ""
    ))
  }

  if (identical(fn, "nchar")) {
    return(translate_nchar_expr(args, context, expr, env = env, fields = fields))
  }

  if (fn %in% c("substr", "substring")) {
    return(translate_substr_expr(args, context, expr, env = env, fields = fields))
  }

  if (identical(fn, "round")) {
    digits <- if (length(args) > 1) {
      translate_local_scalar(
        args[[2]],
        env = env,
        context = context,
        fn = "round",
        validator = function(value) is.numeric(value) && length(value) == 1L,
        details = "only supports literal digits."
      )
    } else {
      0
    }
    return(list(
      type = "round",
      arg = translate_expr(args[[1]], context = context, env = env, fields = fields),
      digits = as.integer(digits)
    ))
  }

  if (identical(fn, "if_else")) {
    if (length(args) < 3) {
      abort_invalid("if_else()", "requires condition, true, and false branches.")
    }
    return(list(
      type = "if_else",
      condition = translate_expr(args[[1]], context = "predicate", env = env, fields = fields),
      true = translate_expr(args[[2]], context = context, env = env, fields = fields),
      false = translate_expr(args[[3]], context = context, env = env, fields = fields)
    ))
  }

  if (identical(fn, "case_when")) {
    formulas <- args
    if (!all(vapply(formulas, rlang::is_formula, logical(1)))) {
      abort_unsupported(context, expr, "case_when() only supports formula branches.")
    }

    cases <- list()
    default <- list(type = "literal", value = NULL)

    for (branch in formulas) {
      lhs <- rlang::f_lhs(branch)
      rhs <- rlang::f_rhs(branch)
      if (isTRUE(lhs)) {
        default <- translate_expr(rhs, context = context, env = env, fields = fields)
      } else {
        cases[[length(cases) + 1]] <- list(
          condition = translate_expr(lhs, context = "predicate", env = env, fields = fields),
          value = translate_expr(rhs, context = context, env = env, fields = fields)
        )
      }
    }

    return(list(type = "case_when", cases = cases, default = default))
  }

  if (identical(fn, "is.na")) {
    return(list(type = "is_na", arg = translate_expr(args[[1]], context = context, env = env, fields = fields)))
  }

  abort_unsupported(context, expr)
}

#' @keywords internal
is_mutate_sequence_expr <- function(expr) {
  if (rlang::is_quosure(expr)) {
    expr <- rlang::get_expr(expr)
  }

  if (!rlang::is_call(expr, ":")) {
    return(FALSE)
  }

  args <- rlang::call_args(expr)
  if (length(args) != 2L) {
    return(FALSE)
  }

  is_scalar_literal(args[[1]]) &&
    is.numeric(args[[1]]) &&
    identical(as.numeric(args[[1]]), 1) &&
    rlang::is_call(args[[2]], "n") &&
    length(rlang::call_args(args[[2]])) == 0L
}

#' @keywords internal
compile_mongo_args <- function(args) {
  unname(lapply(args, compile_mongo_expr))
}

#' @keywords internal
compile_mongo_expr <- function(expr) {
  switch(
    expr$type,
    field = field_reference(expr$name),
    literal = expr$value,
    comparison = stats::setNames(list(compile_mongo_args(expr$args)), paste0("$", expr$fn)),
    boolean = stats::setNames(list(compile_mongo_args(expr$args)), paste0("$", expr$fn)),
    not = list(`$not` = list(compile_mongo_expr(expr$arg))),
    call = stats::setNames(list(compile_mongo_args(expr$args)), paste0("$", expr$fn)),
    round = list(`$round` = list(compile_mongo_expr(expr$arg), expr$digits)),
    if_else = list(`$cond` = list(
      `if` = compile_mongo_expr(expr$condition),
      then = compile_mongo_expr(expr$true),
      `else` = compile_mongo_expr(expr$false)
    )),
    case_when = list(`$switch` = list(
      branches = lapply(expr$cases, function(branch) {
        list(
          case = compile_mongo_expr(branch$condition),
          then = compile_mongo_expr(branch$value)
        )
      }),
      default = compile_mongo_expr(expr$default)
    )),
    is_na = list(`$eq` = list(compile_mongo_expr(expr$arg), NULL)),
    abort_invalid("compile_mongo_expr()", paste("cannot compile expression type", expr$type))
  )
}
