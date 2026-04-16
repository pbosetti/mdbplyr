#' Filter a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Predicate expressions.
#' @param .by Unsupported.
#' @param .preserve Included for dplyr compatibility.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::filter(tbl, amount > 0)
#' @rdname mongo_filter
#' @export
filter.tbl_mongo <- function(.data, ..., .by = NULL, .preserve = FALSE) {
  if (!is.null(.by)) {
    abort_unsupported("filter()", .by, ".by is not supported.")
  }

  predicates <- rlang::enquos(...)
  translated <- lapply(predicates, translate_predicate)
  update_ir(.data, filters = c(.data$ir$filters, translated))
}
