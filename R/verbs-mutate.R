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
#' @details
#' Expression arguments follow the same field-vs-local name resolution rules as
#' [filter.tbl_mongo()].
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
  current_map <- projection_mapping(.data)
  shape <- append_field_map(current_map, names_in, collect_map = .data$ir$collect_map)
  internal_names <- unname(shape$field_map[names_in])

  translated <- lapply(quos[!is_sequence], translate_expr, context = "mutate()", field_map = current_map)
  names(translated) <- internal_names[!is_sequence]

  collect_map <- .data$ir$collect_map
  if (!is.null(collect_map)) {
    keep <- setdiff(names(collect_map), names_in)
    collect_map <- c(collect_map[keep], shape$field_map[names_in])
  }

  row_ops <- .data$ir$row_ops
  if (any(is_sequence)) {
    row_ops <- c(row_ops, list(list(
      type = "sequence",
      fields = internal_names[is_sequence],
      groups = resolve_field_sources(.data$ir$groups, current_map)
    )))
  }

  update_ir(
    .data,
    computed = c(.data$ir$computed, translated),
    field_map = shape$field_map,
    collect_map = collect_map,
    row_ops = row_ops,
    ops = c(.data$ir$ops, list(list(
      type = "mutate",
      computed = translated,
      sequence_fields = internal_names[is_sequence],
      groups = resolve_field_sources(.data$ir$groups, current_map)
    )))
  )
}

#' Compute and keep only derived fields
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Named scalar expressions.
#'
#' @details
#' Expression arguments follow the same field-vs-local name resolution rules as
#' [filter.tbl_mongo()].
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
  current_map <- projection_mapping(.data)
  shape <- append_field_map(character(), names_in)
  internal_names <- unname(shape$field_map[names_in])

  translated <- lapply(quos[!is_sequence], translate_expr, context = "transmute()", field_map = current_map)
  names(translated) <- internal_names[!is_sequence]

  projection <- stats::setNames(internal_names, internal_names)
  row_ops <- .data$ir$row_ops
  if (any(is_sequence)) {
    row_ops <- c(row_ops, list(list(
      type = "sequence",
      fields = internal_names[is_sequence],
      groups = resolve_field_sources(.data$ir$groups, current_map)
    )))
  }

  update_ir(
    .data,
    computed = translated,
    projection = projection,
    field_map = shape$field_map,
    collect_map = shape$field_map,
    row_ops = row_ops,
    ops = c(
      .data$ir$ops,
      list(list(
        type = "mutate",
        computed = translated,
        sequence_fields = internal_names[is_sequence],
        groups = resolve_field_sources(.data$ir$groups, current_map)
      )),
      list(list(type = "project", projection = projection))
    )
  )
}
