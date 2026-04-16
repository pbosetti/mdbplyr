#' @keywords internal
translate_expr <- function(expr, context = "scalar") {
  if (rlang::is_quosure(expr)) {
    expr <- rlang::get_expr(expr)
  }

  if (rlang::is_symbol(expr)) {
    return(list(type = "field", name = rlang::as_string(expr)))
  }

  if (is_scalar_literal(expr)) {
    return(list(type = "literal", value = expr))
  }

  if (!rlang::is_call(expr)) {
    abort_unsupported(context, expr)
  }

  fn <- rlang::call_name(expr)
  args <- rlang::call_args(expr)

  comparison_map <- c(`==` = "eq", `!=` = "ne", `>` = "gt", `>=` = "gte", `<` = "lt", `<=` = "lte")
  boolean_map <- c(`&` = "and", `|` = "or")
  arithmetic_map <- c(`+` = "add", `-` = "subtract", `*` = "multiply", `/` = "divide")
  scalar_map <- c(abs = "abs", sqrt = "sqrt", log = "ln", exp = "exp")

  if (fn %in% names(comparison_map)) {
    return(list(
      type = "comparison",
      fn = comparison_map[[fn]],
      args = lapply(args, translate_expr, context = "predicate")
    ))
  }

  if (fn %in% names(boolean_map)) {
    return(list(
      type = "boolean",
      fn = boolean_map[[fn]],
      args = lapply(args, translate_expr, context = "predicate")
    ))
  }

  if (identical(fn, "!")) {
    return(list(type = "not", arg = translate_expr(args[[1]], context = "predicate")))
  }

  if (fn %in% names(arithmetic_map)) {
    if (length(args) == 1 && identical(fn, "-")) {
      return(list(type = "call", fn = "multiply", args = list(list(type = "literal", value = -1), translate_expr(args[[1]], context = context))))
    }

    return(list(
      type = "call",
      fn = arithmetic_map[[fn]],
      args = lapply(args, translate_expr, context = context)
    ))
  }

  if (fn %in% names(scalar_map)) {
    return(list(type = "call", fn = scalar_map[[fn]], args = lapply(args, translate_expr, context = context)))
  }

  if (identical(fn, "round")) {
    digits <- if (length(args) > 1) args[[2]] else 0
    if (!is_scalar_literal(digits) || !is.numeric(digits)) {
      abort_unsupported(context, expr, "round() only supports literal digits.")
    }
    return(list(
      type = "round",
      arg = translate_expr(args[[1]], context = context),
      digits = as.integer(digits)
    ))
  }

  if (identical(fn, "if_else")) {
    if (length(args) < 3) {
      abort_invalid("if_else()", "requires condition, true, and false branches.")
    }
    return(list(
      type = "if_else",
      condition = translate_expr(args[[1]], context = "predicate"),
      true = translate_expr(args[[2]], context = context),
      false = translate_expr(args[[3]], context = context)
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
        default <- translate_expr(rhs, context = context)
      } else {
        cases[[length(cases) + 1]] <- list(
          condition = translate_expr(lhs, context = "predicate"),
          value = translate_expr(rhs, context = context)
        )
      }
    }

    return(list(type = "case_when", cases = cases, default = default))
  }

  if (identical(fn, "is.na")) {
    return(list(type = "is_na", arg = translate_expr(args[[1]], context = context)))
  }

  abort_unsupported(context, expr)
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
