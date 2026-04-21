#' Create a lazy MongoDB table
#'
#' @param collection A `mongo_src` object or a `mongolite::mongo()`-like object.
#' @param name Optional collection name when `collection` is not already a `mongo_src`.
#' @param schema Optional character vector describing known fields.
#' @param executor Optional executor function for compiled pipelines.
#'
#' @details
#' Supplying `schema = ...` is the most reliable way to make field references
#' explicit, especially for dotted paths in nested documents. If you do not want
#' to write the schema manually, [infer_schema()] can populate it from the first
#' document in the collection:
#'
#' `tbl_mongo(collection) |> infer_schema()`
#'
#' This is convenient, but it only reflects one document. If the collection is
#' heterogeneous, fields that do not appear in the first document may still need
#' to be added manually.
#'
#' @return A `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) {
#'     tibble::tibble(status = "paid", amount = 10)
#'   }
#' )
#'
#' tbl
#' @export
tbl_mongo <- function(collection, name = NULL, schema = NULL, executor = NULL) {
  src <- if (inherits(collection, "mongo_src")) {
    collection
  } else {
    mongo_src(collection, name = name, schema = schema, executor = executor)
  }

  ir <- list(
    schema = unique(schema %||% src$schema %||% character()),
    filters = list(),
    projection = NULL,
    computed = list(),
    groups = character(),
    summaries = list(),
    order = list(),
    slices = list(),
    row_ops = list(),
    manual_stages = list()
  )

  structure(
    list(src = src, ir = ir),
    class = c("tbl_mongo", "tbl")
  )
}

#' @keywords internal
new_tbl_mongo <- function(src, ir) {
  structure(list(src = src, ir = ir), class = c("tbl_mongo", "tbl"))
}

#' @keywords internal
update_ir <- function(x, ...) {
  updates <- list(...)
  ir <- x$ir
  for (name in names(updates)) {
    ir[[name]] <- updates[[name]]
  }
  new_tbl_mongo(x$src, ir)
}

#' @export
print.tbl_mongo <- function(x, ...) {
  cat("<tbl_mongo>", x$src$name, "
", sep = " ")
  cat("  Filters:", length(x$ir$filters), "
")
  cat("  Projection:", if (is.null(x$ir$projection)) "<all>" else paste(names(x$ir$projection), collapse = ", "), "
")
  cat("  Computed:", if (length(x$ir$computed)) paste(names(x$ir$computed), collapse = ", ") else "<none>", "
")
  cat("  Groups:", if (length(x$ir$groups)) paste(x$ir$groups, collapse = ", ") else "<none>", "
")
  cat("  Summaries:", if (length(x$ir$summaries)) paste(names(x$ir$summaries), collapse = ", ") else "<none>", "
")
  slice_label <- if (length(x$ir$slices) == 0) {
    "<none>"
  } else {
    paste(vapply(x$ir$slices, format_slice_label, character(1)), collapse = " -> ")
  }
  cat("  Slice:", slice_label, "
")
  cat("  Manual stages:", length(x$ir$manual_stages), "
")
  invisible(x)
}

#' @keywords internal
format_slice_label <- function(slice) {
  paste0(slice$verb, "(", slice$n, ")")
}
