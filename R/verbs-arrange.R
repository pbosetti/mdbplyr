#' Arrange a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Bare field names or `desc(field)`.
#' @param .by_group Whether to prefix the ordering with grouping fields.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::arrange(tbl, dplyr::desc(amount))
#' @rdname mongo_arrange
#' @export
arrange.tbl_mongo <- function(.data, ..., .by_group = FALSE) {
  quos <- rlang::enquos(...)
  order <- list()

  if (isTRUE(.by_group) && length(.data$ir$groups) > 0) {
    for (group in .data$ir$groups) {
      order[[group]] <- 1L
    }
  }

  for (quo in quos) {
    expr <- rlang::get_expr(quo)
    direction <- 1L
    if (rlang::is_call(expr, "desc")) {
      direction <- -1L
      expr <- rlang::call_args(expr)[[1]]
    }
    if (!rlang::is_symbol(expr)) {
      abort_unsupported("arrange()", expr, "Only bare field names and desc(field) are supported.")
    }
    order[[rlang::as_string(expr)]] <- direction
  }

  update_ir(.data, order = order)
}

#' Limit a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Must be empty.
#' @param n Number of rows to keep.
#' @param prop Unsupported.
#' @param by Unsupported.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::slice_head(tbl, n = 2)
#' head(tbl, 2)
#' @rdname mongo_slice_head
#' @export
slice_head.tbl_mongo <- function(.data, ..., n = NULL, prop = NULL, by = NULL) {
  if (!is.null(prop) || !is.null(by) || dots_n(...) > 0) {
    abort_unsupported("slice_head()", NULL, "Only slice_head(n = ...) is supported.")
  }
  limit <- if (is.null(n)) 6L else as.integer(n)
  update_ir(.data, limit = limit)
}

#' @keywords internal
dots_n <- function(...) {
  length(rlang::list2(...))
}

#' @importFrom utils head
#' @param x A `tbl_mongo` object.
#' @rdname mongo_slice_head
#' @export
head.tbl_mongo <- function(x, n = 6L, ...) {
  update_ir(x, limit = as.integer(n))
}
