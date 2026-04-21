#' Show the MongoDB aggregation pipeline for a lazy query
#'
#' @param x A `tbl_mongo` object.
#' @param ... Unused.
#'
#' @return The pipeline JSON string, invisibly.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' query <- dplyr::select(tbl, amount)
#' show_query(query)
#' @export
show_query <- function(x, ...) {
  UseMethod("show_query")
}

#' @export
show_query.tbl_mongo <- function(x, ...) {
  pipeline <- compile_pipeline(x)
  rendered <- render_pipeline_json(pipeline)
  cat(rendered, "
")
  invisible(rendered)
}
