#' @keywords internal
parse_projection <- function(quos, context = "select()") {
  specs <- list()
  names_in <- rlang::names2(quos)
  for (i in seq_along(quos)) {
    expr <- rlang::get_expr(quos[[i]])
    if (!rlang::is_symbol(expr)) {
      abort_unsupported(context, expr, "Only bare field names are currently supported.")
    }
    src <- rlang::as_string(expr)
    out <- if (nzchar(names_in[[i]])) names_in[[i]] else src
    specs[[out]] <- src
  }
  as_named_character(unlist(specs, use.names = TRUE))
}

#' Select fields from a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Bare field names or `new_name = old_name` renames.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::select(tbl, amount)
#' @rdname mongo_select
#' @export
select.tbl_mongo <- function(.data, ...) {
  quos <- rlang::enquos(...)
  projection <- parse_projection(quos, context = "select()")
  update_ir(.data, projection = projection)
}

#' Rename fields in a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Named bare field renames.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::rename(tbl, total = amount)
#' @rdname mongo_rename
#' @export
rename.tbl_mongo <- function(.data, ...) {
  quos <- rlang::enquos(...)
  rename_specs <- parse_projection(quos, context = "rename()")
  mapping <- projection_mapping(.data)
  if (!length(mapping)) {
    abort_invalid("rename()", "requires known fields. Supply schema when creating tbl_mongo().")
  }

  for (new_name in names(rename_specs)) {
    old_name <- unname(rename_specs[[new_name]])
    if (!old_name %in% names(mapping)) {
      abort_invalid("rename()", paste0("cannot rename unknown field `", old_name, "`."))
    }
    names(mapping)[names(mapping) == old_name] <- new_name
  }

  update_ir(.data, projection = as_named_character(mapping))
}
