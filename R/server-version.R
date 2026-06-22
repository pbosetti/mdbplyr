#' @keywords internal
as_server_version <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }
  if (inherits(x, "numeric_version")) {
    return(x)
  }
  tryCatch(numeric_version(x), error = function(...) NULL)
}

#' @keywords internal
detect_server_version <- function(collection) {
  run <- tryCatch(collection$run, error = function(...) NULL)
  if (!is.function(run)) {
    return(NULL)
  }

  info <- tryCatch(run('{"buildInfo":1}'), error = function(...) NULL)
  if (is.null(info)) {
    return(NULL)
  }

  version <- tryCatch(info$version, error = function(...) NULL)
  as_server_version(version)
}

#' Report the MongoDB server version backing a source
#'
#' @param x A `mongo_src` or `tbl_mongo` object.
#'
#' @details
#' The version is resolved when the source is created, either from an explicit
#' `server_version` argument to [mongo_src()] or by probing the connected server
#' with `buildInfo`. Test doubles and offline executors that cannot answer the
#' probe report `NULL`.
#'
#' @return A `numeric_version`, or `NULL` when the version is unknown.
#' @examples
#' src <- mongo_src(
#'   list(name = "orders", aggregate = function(...) tibble::tibble()),
#'   schema = c("status", "amount"),
#'   server_version = "7.0"
#' )
#'
#' mongo_server_version(src)
#' @export
mongo_server_version <- function(x) {
  UseMethod("mongo_server_version")
}

#' @export
mongo_server_version.mongo_src <- function(x) {
  x$server_version
}

#' @export
mongo_server_version.tbl_mongo <- function(x) {
  mongo_server_version(x$src)
}

#' @keywords internal
require_server_version <- function(x, min_version, feature, allow_unknown = TRUE) {
  min_version <- as_server_version(min_version)
  if (is.null(min_version)) {
    abort_invalid("require_server_version()", "`min_version` must be a valid version string.")
  }

  version <- mongo_server_version(x)

  if (is.null(version)) {
    if (isTRUE(allow_unknown)) {
      return(invisible(NULL))
    }
    cli::cli_abort(
      c(
        "{feature} requires MongoDB {format(min_version)} or newer.",
        i = "The server version could not be determined; pass {.code server_version=} to {.fn mongo_src}."
      ),
      class = "mongo_tidy_unsupported"
    )
  }

  if (version < min_version) {
    cli::cli_abort(
      c(
        "{feature} requires MongoDB {format(min_version)} or newer.",
        x = "The connected server reports version {format(version)}."
      ),
      class = "mongo_tidy_unsupported"
    )
  }

  invisible(NULL)
}
