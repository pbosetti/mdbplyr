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
#' @details
#' Selecting a dotted field path such as `` `message.measurements.Fx` `` does
#' not flatten nested documents by default. The collected result preserves the
#' native nested structure unless you explicitly rename the field in `select()`
#' or call [flatten_fields()].
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
  visible_sources <- parse_projection(quos, context = "select()")
  current_map <- projection_mapping(.data)
  identity_select <- is.null(.data$ir$collect_map) &&
    all(names(visible_sources) == unname(visible_sources)) &&
    all(resolve_field_sources(unname(visible_sources), current_map) == unname(visible_sources))

  if (isTRUE(identity_select)) {
    projection <- as_named_character(visible_sources)
    field_map <- as_named_character(visible_sources)
    collect_map <- NULL
  } else {
    shape <- build_projection_shape(current_map, visible_sources)
    projection <- shape$projection
    field_map <- shape$field_map
    collect_map <- shape$field_map
  }

  update_ir(
    .data,
    projection = projection,
    field_map = field_map,
    collect_map = collect_map,
    ops = c(.data$ir$ops, list(list(type = "project", projection = projection)))
  )
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
  current_map <- projection_mapping(.data)
  if (!length(current_map)) {
    abort_invalid("rename()", "requires known fields. Supply schema when creating tbl_mongo().")
  }

  for (new_name in names(rename_specs)) {
    old_name <- unname(rename_specs[[new_name]])
    if (!old_name %in% names(current_map)) {
      abort_invalid("rename()", paste0("cannot rename unknown field `", old_name, "`."))
    }
  }

  output_names <- names(current_map)
  for (new_name in names(rename_specs)) {
    old_name <- unname(rename_specs[[new_name]])
    output_names[output_names == old_name] <- new_name
  }
  visible_sources <- stats::setNames(names(current_map), output_names)
  shape <- build_projection_shape(current_map, visible_sources)

  update_ir(
    .data,
    projection = shape$projection,
    field_map = shape$field_map,
    collect_map = shape$field_map,
    ops = c(.data$ir$ops, list(list(type = "project", projection = shape$projection)))
  )
}
