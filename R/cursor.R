#' Open a lazy Mongo query as a mongolite cursor
#'
#' @param x A `tbl_mongo` object.
#' @param ... Additional arguments forwarded to the cursor executor.
#'
#' @return A `mongolite` iterator when backed by a live MongoDB collection, or a
#'   compatible cursor-like object supplied by the source.
#' @examples
#' collection <- list(
#'   name = "orders",
#'   aggregate = function(pipeline_json, iterate = FALSE, ...) {
#'     data <- tibble::tibble(status = "paid", amount = 10)
#'     if (!iterate) {
#'       return(data)
#'     }
#'
#'     local({
#'       offset <- 1L
#'       page <- function(size = 1000) {
#'         if (offset > nrow(data)) {
#'           return(data[0, , drop = FALSE])
#'         }
#'         out <- data[offset:min(nrow(data), offset + size - 1L), , drop = FALSE]
#'         offset <<- offset + nrow(out)
#'         tibble::as_tibble(out)
#'       }
#'       structure(environment(), class = "mongo_iter")
#'     })
#'   }
#' )
#'
#' tbl <- tbl_mongo(collection, schema = c("status", "amount"))
#' iter <- cursor(dplyr::filter(tbl, amount > 0))
#' iter$page()
#' @export
cursor <- function(x, ...) {
  if (inherits(x, "tbl_mongo")) {
    return(cursor.tbl_mongo(x, ...))
  }
  UseMethod("cursor")
}

#' @export
cursor.tbl_mongo <- function(x, ...) {
  if (!is.function(x$src$cursor_executor)) {
    abort_invalid(
      "cursor()",
      "requires a collection with an $aggregate(..., iterate = TRUE) method or an explicit cursor executor."
    )
  }

  pipeline <- compile_pipeline(x)
  x$src$cursor_executor(pipeline, ...)
}
