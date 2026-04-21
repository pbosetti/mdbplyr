#' Group a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Bare field names.
#' @param .add Whether to add to existing groups.
#' @param .drop Included for dplyr compatibility.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::group_by(tbl, status)
#' @rdname mongo_group_by
#' @export
group_by.tbl_mongo <- function(.data, ..., .add = FALSE, .drop = dplyr::group_by_drop_default(.data)) {
  quos <- rlang::enquos(...)
  groups <- vapply(quos, function(quo) {
    expr <- rlang::get_expr(quo)
    if (!rlang::is_symbol(expr)) {
      abort_unsupported("group_by()", expr, "Only bare field names are supported.")
    }
    rlang::as_string(expr)
  }, character(1))

  if (isTRUE(.add)) {
    groups <- unique(c(.data$ir$groups, groups))
  }

  update_ir(.data, groups = groups)
}

#' Summarise a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Named summary expressions.
#' @param .by Unsupported.
#' @param .groups Included for dplyr compatibility.
#'
#' @details
#' Summary expressions follow the same field-vs-local name resolution rules as
#' [filter.tbl_mongo()].
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' query <- tbl |>
#'   dplyr::group_by(status) |>
#'   dplyr::summarise(total = sum(amount))
#'
#' show_query(query)
#' @rdname mongo_summarise
#' @export
summarise.tbl_mongo <- function(.data, ..., .by = NULL, .groups = NULL) {
  if (!is.null(.by)) {
    abort_unsupported("summarise()", .by, ".by is not supported.")
  }

  quos <- rlang::enquos(...)
  if (!length(quos)) {
    abort_invalid("summarise()", "requires at least one summary expression.")
  }

  names_in <- rlang::names2(quos)
  names_in[names_in == ""] <- vapply(quos[names_in == ""], expr_text, character(1))
  translated <- lapply(quos, translate_agg, fields = schema_fields(.data))
  names(translated) <- names_in
  update_ir(.data, summaries = translated)
}
