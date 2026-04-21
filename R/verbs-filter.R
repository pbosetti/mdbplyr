#' Filter a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Predicate expressions.
#' @param .by Unsupported.
#' @param .preserve Included for dplyr compatibility.
#'
#' @details
#' Predicate expressions use schema-first tidy evaluation:
#'
#' - bare names refer to MongoDB fields when they are known fields of the lazy
#'   table
#' - otherwise, bare names are evaluated in the local R environment and inlined
#'   as literals
#' - `.data$...` always forces a MongoDB field reference
#' - `.env$...` always forces a local R value
#' - if a name exists both as a field and as a local variable, the field wins;
#'   use `.env$...` to force the local value
#'
#' Bare field resolution depends on the known schema of `.data`. If a field,
#' including a dotted path such as `` `message.measurements.Fx` ``, is missing
#' from [schema_fields()], write it explicitly as `.data$...` or supply the
#' schema when creating the `tbl_mongo`.
#'
#' The same resolution rules apply to expression arguments in
#' [mutate.tbl_mongo()] and [summarise.tbl_mongo()].
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
#'
#' threshold <- 10
#' dplyr::filter(tbl, amount > threshold)
#' dplyr::filter(tbl, .data$amount > .env$threshold)
#' @rdname mongo_filter
#' @export
filter.tbl_mongo <- function(.data, ..., .by = NULL, .preserve = FALSE) {
  if (!is.null(.by)) {
    abort_unsupported("filter()", .by, ".by is not supported.")
  }

  predicates <- rlang::enquos(...)
  translated <- lapply(predicates, translate_predicate, fields = schema_fields(.data))
  update_ir(.data, filters = c(.data$ir$filters, translated))
}
