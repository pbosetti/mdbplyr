#' @keywords internal
append_projection_fields <- function(projection, fields) {
  if (is.null(projection)) {
    return(NULL)
  }
  for (field in fields) {
    projection[[field]] <- field
  }
  as_named_character(projection)
}

#' Add computed fields to a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Named scalar expressions.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::mutate(tbl, doubled = amount * 2)
#' @rdname mongo_mutate
#' @export
mutate.tbl_mongo <- function(.data, ...) {
  quos <- rlang::enquos(...)
  if (!length(quos)) {
    return(.data)
  }

  names_in <- rlang::names2(quos)
  if (any(!nzchar(names_in))) {
    abort_invalid("mutate()", "requires named expressions.")
  }

  is_sequence <- vapply(quos, is_mutate_sequence_expr, logical(1))
  translated <- lapply(quos[!is_sequence], translate_expr, context = "mutate()")
  names(translated) <- names_in[!is_sequence]
  projection <- append_projection_fields(.data$ir$projection, names_in)
  row_ops <- .data$ir$row_ops
  if (any(is_sequence)) {
    row_ops <- c(row_ops, list(list(
      type = "sequence",
      fields = names_in[is_sequence],
      groups = .data$ir$groups
    )))
  }

  update_ir(.data, computed = c(.data$ir$computed, translated), projection = projection, row_ops = row_ops)
}

#' Compute and keep only derived fields
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Named scalar expressions.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::transmute(tbl, doubled = amount * 2)
#' @rdname mongo_transmute
#' @export
transmute.tbl_mongo <- function(.data, ...) {
  quos <- rlang::enquos(...)
  if (!length(quos)) {
    return(.data)
  }

  names_in <- rlang::names2(quos)
  if (any(!nzchar(names_in))) {
    abort_invalid("transmute()", "requires named expressions.")
  }

  is_sequence <- vapply(quos, is_mutate_sequence_expr, logical(1))
  translated <- lapply(quos[!is_sequence], translate_expr, context = "transmute()")
  names(translated) <- names_in[!is_sequence]
  projection <- stats::setNames(names_in, names_in)
  row_ops <- .data$ir$row_ops
  if (any(is_sequence)) {
    row_ops <- c(row_ops, list(list(
      type = "sequence",
      fields = names_in[is_sequence],
      groups = .data$ir$groups
    )))
  }

  update_ir(.data, computed = translated, projection = projection, row_ops = row_ops)
}
