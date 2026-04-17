#' Construct a MongoDB source wrapper
#'
#' @param collection A `mongolite::mongo()`-like object or a test double.
#' @param name Optional human-readable collection name.
#' @param schema Optional character vector describing the available fields.
#' @param executor Optional function used to execute compiled pipelines.
#' @param cursor_executor Optional function used to open a cursor over compiled
#'   pipelines.
#'
#' @return A `mongo_src` object.
#' @examples
#' collection <- list(
#'   name = "orders",
#'   aggregate = function(pipeline_json, iterate = FALSE, ...) {
#'     if (iterate) {
#'       data <- tibble::tibble(status = "paid", amount = 10)
#'       return(local({
#'         page <- function(size = 1000) data
#'         structure(environment(), class = "mongo_iter")
#'       }))
#'     }
#'     tibble::tibble(status = "paid", amount = 10)
#'   }
#' )
#'
#' src <- mongo_src(collection, schema = c("status", "amount"))
#' src
#' @export
mongo_src <- function(collection, name = NULL, schema = NULL, executor = NULL, cursor_executor = NULL) {
  if (is.null(executor)) {
    aggregate_method <- tryCatch(collection$aggregate, error = function(...) NULL)
    if (is.function(aggregate_method)) {
      executor <- function(pipeline, ...) {
        aggregate_method(jsonlite::toJSON(
          pipeline,
          auto_unbox = TRUE,
          null = "null",
          pretty = FALSE
        ), ...)
      }
    }
  }

  if (is.null(cursor_executor)) {
    aggregate_method <- tryCatch(collection$aggregate, error = function(...) NULL)
    if (is.function(aggregate_method)) {
      cursor_executor <- function(pipeline, ...) {
        aggregate_method(jsonlite::toJSON(
          pipeline,
          auto_unbox = TRUE,
          null = "null",
          pretty = FALSE
        ), iterate = TRUE, ...)
      }
    }
  }

  if (!is.function(executor)) {
    abort_invalid("mongo_src()", "requires an executor or a collection with an $aggregate() method.")
  }

  if (is.null(schema)) {
    data <- tryCatch(collection$data, error = function(...) NULL)
    if (is.data.frame(data)) {
      schema <- names(data)
    }
  }

  structure(
    list(
      collection = collection,
      name = name %||% tryCatch(collection$name, error = function(...) "collection"),
      schema = unique(schema %||% character()),
      executor = executor,
      cursor_executor = cursor_executor
    ),
    class = "mongo_src"
  )
}

#' @export
print.mongo_src <- function(x, ...) {
  cat("<mongo_src>", x$name, "
", sep = " ")
  cat("  Fields:", if (length(x$schema)) paste(x$schema, collapse = ", ") else "<unknown>", "
")
  invisible(x)
}
