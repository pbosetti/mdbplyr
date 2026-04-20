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

  update_ir(
    .data,
    order = order,
    row_ops = c(.data$ir$row_ops, list(list(type = "sort", order = order)))
  )
}

#' Slice a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Must be empty.
#' @param n Number of rows to keep. Negative values drop rows from the opposite
#'   end, matching `dplyr`.
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
#' dplyr::slice_tail(tbl, n = 2)
#' head(tbl, 2)
#' @rdname mongo_slice_head
#' @export
slice_head.tbl_mongo <- function(.data, ..., n = NULL, prop = NULL, by = NULL) {
  if (!is.null(prop) || !is.null(by) || dots_n(...) > 0) {
    abort_unsupported("slice_head()", NULL, "Only slice_head(n = ...) is supported.")
  }
  append_slice(.data, validate_slice_n(n, default_n = 1L, verb = "head", context = "slice_head()"))
}

#' @rdname mongo_slice_head
#' @export
slice_tail.tbl_mongo <- function(.data, ..., n = NULL, prop = NULL, by = NULL) {
  if (!is.null(prop) || !is.null(by) || dots_n(...) > 0) {
    abort_unsupported("slice_tail()", NULL, "Only slice_tail(n = ...) is supported.")
  }
  append_slice(.data, validate_slice_n(n, default_n = 1L, verb = "tail", context = "slice_tail()"))
}

#' @keywords internal
dots_n <- function(...) {
  length(rlang::list2(...))
}

#' @keywords internal
validate_slice_n <- function(n, default_n, verb, context) {
  if (is.null(n)) {
    return(list(verb = verb, n = default_n))
  }

  if (!is.numeric(n) || length(n) != 1L || is.na(n) || n != trunc(n)) {
    abort_invalid(context, "`n` must be a single round number.")
  }

  list(verb = verb, n = as.integer(n))
}

#' @keywords internal
append_slice <- function(.data, slice) {
  update_ir(
    .data,
    slices = c(.data$ir$slices, list(slice)),
    row_ops = c(.data$ir$row_ops, list(list(type = "slice", slice = slice)))
  )
}

#' @importFrom utils head
#' @param x A `tbl_mongo` object.
#' @rdname mongo_slice_head
#' @export
head.tbl_mongo <- function(x, n = 6L, ...) {
  append_slice(x, validate_slice_n(n, default_n = 6L, verb = "head", context = "head()"))
}
