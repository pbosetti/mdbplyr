#' @keywords internal
translate_predicate <- function(expr, fields = NULL) {
  translated <- translate_expr(expr, context = "predicate", fields = fields)
  allowed <- c("comparison", "boolean", "not", "is_na", "literal", "field")
  if (!translated$type %in% allowed) {
    abort_unsupported("filter()", expr, "Predicates must evaluate to logical comparisons.")
  }
  translated
}
