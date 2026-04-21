#' Inspect known fields for a lazy Mongo query
#'
#' @param x A `tbl_mongo` or `mongo_src` object.
#'
#' @return A character vector of known field names.
#' @examples
#' src <- mongo_src(
#'   list(name = "orders", aggregate = function(...) tibble::tibble()),
#'   schema = c("status", "amount")
#' )
#' tbl <- tbl_mongo(src)
#'
#' schema_fields(src)
#' schema_fields(tbl)
#' @export
schema_fields <- function(x) {
  UseMethod("schema_fields")
}

#' Infer schema fields from the first source document
#'
#' @param x A `tbl_mongo` object.
#'
#' @return A `tbl_mongo` object with its source and IR schema updated from the
#'   first document in the underlying collection.
#' @details
#' `infer_schema()` inspects the first document of the source collection and
#' flattens nested named subdocuments into dotted paths such as
#' `"message.measurements.Fx"`. Arrays and other non-object values are treated
#' as leaf fields. Because the schema comes from a single document, heterogeneous
#' collections may still require manual schema adjustment.
#' @examples
#' collection <- list(
#'   name = "orders",
#'   aggregate = function(pipeline_json, iterate = FALSE, ...) {
#'     tibble::tibble(
#'       status = "paid",
#'       message = list(list(amount = 10, currency = "EUR"))
#'     )
#'   }
#' )
#' tbl <- tbl_mongo(collection)
#'
#' schema_fields(infer_schema(tbl))
#' @export
infer_schema <- function(x) {
  UseMethod("infer_schema")
}

#' @export
schema_fields.mongo_src <- function(x) {
  x$schema %||% character()
}

#' @export
schema_fields.tbl_mongo <- function(x) {
  ir <- x$ir
  if (!is.null(ir$field_map)) {
    return(names(ir$field_map))
  }

  fields <- ir$schema %||% character()
  if (!is.null(ir$projection)) {
    fields <- names(ir$projection)
  }
  if (length(ir$computed) > 0) {
    fields <- unique(c(fields, names(ir$computed)))
  }
  fields
}

#' @export
infer_schema.tbl_mongo <- function(x) {
  inferred <- infer_source_schema(x$src)

  src <- x$src
  src$schema <- inferred
  update_ir(
    new_tbl_mongo(src, x$ir),
    schema = inferred,
    field_map = stats::setNames(inferred, inferred)
  )
}

#' @keywords internal
projection_mapping <- function(x) {
  ir <- x$ir
  if (!is.null(ir$field_map)) {
    return(ir$field_map)
  }

  if (!is.null(ir$projection)) {
    return(ir$projection)
  }

  fields <- ir$schema %||% character()
  mapping <- stats::setNames(fields, fields)
  if (length(ir$computed) > 0) {
    computed_names <- names(ir$computed)
    mapping <- c(mapping, stats::setNames(computed_names, computed_names))
  }
  mapping
}

#' @keywords internal
choose_output_name <- function(visible, source_visible = NULL, source_actual = NULL, field_map = character(), used = character()) {
  if (!is.null(source_visible) &&
      identical(source_visible, visible) &&
      !is.null(source_actual) &&
      source_visible %in% names(field_map) &&
      identical(unname(field_map[source_visible]), source_actual) &&
      is_safe_output_name(source_actual) &&
      !source_actual %in% used) {
    return(source_actual)
  }

  if (is_safe_output_name(visible) && !visible %in% used) {
    return(visible)
  }

  allocate_output_name(used)
}

#' @keywords internal
build_projection_shape <- function(current_map, visible_sources) {
  projection <- character()
  field_map <- character()
  used <- character()

  for (visible in names(visible_sources)) {
    source_visible <- unname(visible_sources[[visible]])
    source_actual <- resolve_field_sources(source_visible, current_map)
    output_name <- choose_output_name(
      visible = visible,
      source_visible = source_visible,
      source_actual = source_actual,
      field_map = current_map,
      used = used
    )
    projection[[output_name]] <- source_actual
    field_map[[visible]] <- output_name
    used <- c(used, output_name)
  }

  list(
    projection = as_named_character(projection),
    field_map = as_named_character(field_map)
  )
}

#' @keywords internal
append_field_map <- function(current_map, visible_names, collect_map = NULL) {
  field_map <- current_map
  if (is.null(field_map)) {
    field_map <- character()
  }

  used <- unique(unname(c(field_map, collect_map %||% character())))
  added <- character()

  for (visible in visible_names) {
    source_actual <- resolve_field_sources(visible, field_map)
    existing_actual <- unname(field_map[visible])
    used_now <- if (length(existing_actual) && nzchar(existing_actual)) {
      setdiff(used, existing_actual)
    } else {
      used
    }
    output_name <- choose_output_name(
      visible = visible,
      source_visible = visible,
      source_actual = source_actual,
      field_map = field_map,
      used = used_now
    )
    field_map[[visible]] <- output_name
    added[[visible]] <- output_name
    used <- c(used, output_name)
  }

  list(
    field_map = as_named_character(field_map),
    added = as_named_character(added)
  )
}

#' @keywords internal
resolve_field_sources <- function(names, field_map) {
  if (!length(names)) {
    return(character())
  }

  if (is.null(field_map)) {
    return(names)
  }

  values <- unname(field_map[names])
  missing <- is.na(values) | !nzchar(values)
  values[missing] <- names[missing]
  values
}

#' @keywords internal
infer_source_schema <- function(src) {
  doc <- source_first_document(src)
  inferred <- flatten_document_fields(doc)

  if (!length(inferred)) {
    abort_invalid("infer_schema()", "the first source document does not expose any named fields.")
  }

  unique(inferred)
}

#' @keywords internal
source_first_document <- function(src) {
  data <- src$executor(list(list(`$limit` = 1L)))
  if (!is.data.frame(data) || nrow(data) == 0L) {
    abort_invalid("infer_schema()", "cannot infer schema from an empty collection.")
  }

  row_to_document(data[1, , drop = FALSE])
}

#' @keywords internal
row_to_document <- function(row) {
  stats::setNames(lapply(names(row), function(name) unwrap_document_value(row[[name]])), names(row))
}

#' @keywords internal
unwrap_document_value <- function(value) {
  if (is.data.frame(value)) {
    if (nrow(value) == 1L) {
      return(row_to_document(value[1, , drop = FALSE]))
    }
    return(value)
  }

  if (is.list(value) &&
      length(value) == 1L &&
      (is.null(names(value)) || !nzchar(names(value)[[1L]]))) {
    inner <- value[[1L]]
    if (is.list(inner) || is.data.frame(inner)) {
      return(unwrap_document_value(inner))
    }
  }

  value
}

#' @keywords internal
flatten_document_fields <- function(doc, prefix = NULL) {
  if (is.data.frame(doc)) {
    doc <- row_to_document(doc[1, , drop = FALSE])
  }

  if (!(is.list(doc) && length(names(doc)) > 0L && all(nzchar(names(doc))))) {
    if (is.null(prefix)) {
      return(character())
    }
    return(prefix)
  }

  fields <- unlist(lapply(names(doc), function(name) {
    path <- if (is.null(prefix)) name else paste(prefix, name, sep = ".")
    value <- doc[[name]]

    if (is.data.frame(value) || (is.list(value) && length(names(value)) > 0L && all(nzchar(names(value))))) {
      flatten_document_fields(value, prefix = path)
    } else {
      path
    }
  }), use.names = FALSE)

  unname(fields)
}
