#' Collect a lazy Mongo query
#'
#' @param x A `tbl_mongo` object.
#' @param ... Additional arguments forwarded to the executor.
#'
#' @return A tibble.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) {
#'     tibble::tibble(status = "paid", amount = 10)
#'   }
#' )
#'
#' query <- dplyr::filter(tbl, amount > 0)
#' collect(query)
#' @export
collect <- function(x, ...) {
  UseMethod("collect")
}

#' @export
collect.tbl_mongo <- function(x, ...) {
  pipeline <- compile_pipeline(x)
  result <- x$src$executor(pipeline, ...)
  if (inherits(result, "tbl_df")) {
    return(result)
  }
  tibble::as_tibble(result)
}
