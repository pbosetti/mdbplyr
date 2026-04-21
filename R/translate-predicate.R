#' @keywords internal
translate_predicate <- function(expr, fields = NULL, field_map = NULL) {
  translated <- translate_expr(expr, context = "predicate", fields = fields, field_map = field_map)
  allowed <- c("comparison", "boolean", "not", "is_na", "literal", "field")
  if (!translated$type %in% allowed) {
    abort_unsupported("filter()", expr, "Predicates must evaluate to logical comparisons.")
  }
  translated
}
