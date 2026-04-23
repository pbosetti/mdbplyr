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

#' @keywords internal
translate_mutate_assignments <- function(quos, names_in, is_sequence, current_map, output_map, context, groups) {
  translation_map <- current_map %||% character()
  translated <- list()
  steps <- list()

  for (i in seq_along(quos)) {
    visible_name <- names_in[[i]]
    internal_name <- unname(output_map[[visible_name]])

    if (isTRUE(is_sequence[[i]])) {
      steps[[length(steps) + 1L]] <- list(
        type = "sequence",
        fields = internal_name,
        groups = groups
      )
    } else {
      expr <- translate_expr(quos[[i]], context = context, field_map = translation_map)
      translated[[internal_name]] <- expr
      steps[[length(steps) + 1L]] <- list(
        type = "computed",
        field = internal_name,
        expr = expr
      )
    }

    translation_map[[visible_name]] <- internal_name
  }

  list(
    translated = translated,
    steps = steps
  )
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
  group_sources <- resolve_field_sources(.data$ir$groups, current_map)
  internal_names <- unname(shape$field_map[names_in])

  translated <- translate_mutate_assignments(
    quos = quos,
    names_in = names_in,
    is_sequence = is_sequence,
    current_map = current_map,
    output_map = shape$field_map,
    context = "mutate()",
    groups = group_sources
  )

  collect_map <- .data$ir$collect_map
  if (!is.null(collect_map)) {
    keep <- setdiff(names(collect_map), names_in)
    collect_map <- c(collect_map[keep], shape$field_map[names_in])
  }

  update_ir(
    .data,
    computed = c(.data$ir$computed, translated$translated),
    field_map = shape$field_map,
    collect_map = collect_map,
    ops = c(.data$ir$ops, list(list(
      type = "mutate",
      computed = translated$translated,
      steps = translated$steps,
      sequence_fields = internal_names[is_sequence],
      groups = group_sources
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
  group_sources <- resolve_field_sources(.data$ir$groups, current_map)
  internal_names <- unname(shape$field_map[names_in])

  translated <- translate_mutate_assignments(
    quos = quos,
    names_in = names_in,
    is_sequence = is_sequence,
    current_map = current_map,
    output_map = shape$field_map,
    context = "transmute()",
    groups = group_sources
  )

  projection <- stats::setNames(internal_names, internal_names)

  update_ir(
    .data,
    computed = translated$translated,
    projection = projection,
    field_map = shape$field_map,
    collect_map = shape$field_map,
    ops = c(
      .data$ir$ops,
      list(list(
        type = "mutate",
        computed = translated$translated,
        steps = translated$steps,
        sequence_fields = internal_names[is_sequence],
        groups = group_sources
      )),
      list(list(type = "project", projection = projection))
    )
  )
}
