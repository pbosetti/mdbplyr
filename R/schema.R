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

#' @export
schema_fields.mongo_src <- function(x) {
  x$schema %||% character()
}

#' @export
schema_fields.tbl_mongo <- function(x) {
  ir <- x$ir
  if (length(ir$summaries) > 0) {
    return(unique(c(ir$groups, names(ir$summaries))))
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

#' @keywords internal
projection_mapping <- function(x) {
  ir <- x$ir
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
