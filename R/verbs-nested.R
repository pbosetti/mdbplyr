#' Flatten nested object fields into flat columns
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Optional bare field roots or backticked dotted paths to flatten.
#' @param names_fn Optional naming function applied to flattened output names.
#'
#' @details
#' `flatten_fields()` relies on known schema fields. If nested dotted paths are
#' not known yet, supply `schema = ...` when creating the table or call
#' [infer_schema()] first.
#'
#' With no field arguments, all known dotted paths are flattened. Existing
#' already-flat columns are preserved. Arrays are treated as leaf values; use
#' [unwind_array()] first if you need one row per array element.
#'
#' @return A modified `tbl_mongo` object.
#' @export
flatten_fields <- function(.data, ..., names_fn = identity) {
  UseMethod("flatten_fields")
}

#' @export
flatten_fields.tbl_mongo <- function(.data, ..., names_fn = identity) {
  current_map <- projection_mapping(.data)
  visible_fields <- schema_fields(.data)
  source_schema <- .data$ir$schema %||% character()
  flattenable <- flattenable_field_map(visible_fields, current_map, source_schema)

  if (!length(flattenable$all)) {
    abort_invalid(
      "flatten_fields()",
      "requires known nested dotted paths. Supply schema when creating tbl_mongo() or call infer_schema()."
    )
  }

  quos <- rlang::enquos(...)
  selected <- if (!length(quos)) {
    flattenable$all
  } else {
    flatten_targets(quos, flattenable)
  }

  flattened_names <- names_fn(names(selected))
  if (!is.character(flattened_names) || length(flattened_names) != length(selected)) {
    abort_invalid("flatten_fields()", "`names_fn` must return a character vector with one name per flattened field.")
  }
  if (any(!nzchar(flattened_names))) {
    abort_invalid("flatten_fields()", "`names_fn` must not produce empty names.")
  }

  replaced_fields <- names(quos)
  if (!length(replaced_fields)) {
    replaced_fields <- flattenable$targets
  } else {
    replaced_fields <- vapply(quos, parse_field_name, character(1), context = "flatten_fields()")
  }

  preserved <- visible_fields[!vapply(visible_fields, function(field) {
    any(field == replaced_fields | startsWith(field, paste0(replaced_fields, ".")))
  }, logical(1))]
  output_names <- c(preserved, flattened_names)
  if (anyDuplicated(output_names)) {
    abort_invalid("flatten_fields()", "`names_fn` must produce unique output names that do not collide with preserved columns.")
  }

  visible_sources <- c(
    stats::setNames(preserved, preserved),
    stats::setNames(unname(selected), flattened_names)
  )
  shape <- build_projection_shape(current_map, visible_sources)

  update_ir(
    .data,
    projection = shape$projection,
    field_map = shape$field_map,
    collect_map = shape$field_map,
    ops = c(.data$ir$ops, list(list(type = "project", projection = shape$projection)))
  )
}

#' Unwind one array field lazily
#'
#' @param .data A `tbl_mongo` object.
#' @param field A single bare field name or backticked dotted path.
#' @param preserve_empty Whether to preserve rows with missing or empty arrays.
#'
#' @details
#' `unwind_array()` compiles to MongoDB `$unwind` and repeats each row once per
#' array element, replacing the original array field with that element. Only one
#' field can be unwound per call; chain multiple calls if needed.
#'
#' @return A modified `tbl_mongo` object.
#' @export
unwind_array <- function(.data, field, preserve_empty = FALSE) {
  UseMethod("unwind_array")
}

#' @export
unwind_array.tbl_mongo <- function(.data, field, preserve_empty = FALSE) {
  if (!is.logical(preserve_empty) || length(preserve_empty) != 1L || is.na(preserve_empty)) {
    abort_invalid("unwind_array()", "`preserve_empty` must be TRUE or FALSE.")
  }

  field_name <- parse_field_name(rlang::enquo(field), "unwind_array()")
  current_map <- projection_mapping(.data)
  known_fields <- schema_fields(.data)

  if (!field_name %in% known_fields) {
    abort_invalid(
      "unwind_array()",
      paste0(
        "cannot unwind unknown field `", field_name,
        "`. Supply schema when creating tbl_mongo() or call infer_schema()."
      )
    )
  }

  update_ir(
    .data,
    ops = c(.data$ir$ops, list(list(
      type = "unwind",
      field = field_name,
      field_source = resolve_field_sources(field_name, current_map),
      preserve_empty = isTRUE(preserve_empty)
    )))
  )
}

#' @keywords internal
flatten_targets <- function(quos, flattenable) {
  selected <- character()

  for (quo in quos) {
    target <- parse_field_name(quo, "flatten_fields()")
    matches <- flattenable$by_target[[target]]
    if (is.null(matches) || !length(matches)) {
      abort_invalid(
        "flatten_fields()",
        paste0(
          "cannot flatten unknown nested field `", target,
          "`. Supply schema when creating tbl_mongo() or call infer_schema()."
        )
      )
    }
    selected <- c(selected, matches)
  }

  selected[!duplicated(names(selected))]
}

#' @keywords internal
flattenable_field_map <- function(visible_fields, current_map, source_schema) {
  by_target <- list()
  all <- character()

  for (visible in visible_fields) {
    source <- resolve_field_sources(visible, current_map)
    descendants <- source_schema[startsWith(source_schema, paste0(source, "."))]

    matches <- if (length(descendants)) {
      suffix <- substring(descendants, nchar(source) + 1L)
      stats::setNames(descendants, paste0(visible, suffix))
    } else if (grepl("\\.", visible) && source %in% source_schema) {
      stats::setNames(source, visible)
    } else {
      character()
    }

    by_target[[visible]] <- matches
    all <- c(all, matches)
  }

  list(
    by_target = by_target,
    all = all[!duplicated(names(all))],
    targets = visible_fields[vapply(by_target, length, integer(1)) > 0L]
  )
}

#' @keywords internal
parse_field_name <- function(quo, context) {
  expr <- rlang::get_expr(quo)

  if (rlang::is_symbol(expr)) {
    return(rlang::as_string(expr))
  }

  if (rlang::is_call(expr, "$")) {
    args <- rlang::call_args(expr)
    if (length(args) == 2L && rlang::is_symbol(args[[1]], ".data") && rlang::is_symbol(args[[2]])) {
      return(rlang::as_string(args[[2]]))
    }
  }

  abort_unsupported(context, expr, "Only bare field names, backticked dotted paths, or .data$col are supported.")
}
