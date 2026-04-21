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
  out <- if (inherits(result, "tbl_df")) result else tibble::as_tibble(result)
  postprocess_collect(out, x$ir$collect_map)
}

#' @keywords internal
postprocess_collect <- function(data, collect_map) {
  if (is.null(collect_map) || !length(collect_map)) {
    return(tibble::as_tibble(data))
  }

  internal_names <- unname(collect_map)
  present <- internal_names %in% names(data)
  if (!any(present)) {
    return(tibble::as_tibble(data))
  }

  out <- tibble::as_tibble(data[, internal_names[present], drop = FALSE])
  names(out) <- names(collect_map)[present]
  out
}
