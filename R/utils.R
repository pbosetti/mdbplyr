#' @keywords internal
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' @keywords internal
abort_unsupported <- function(context, expr = NULL, details = NULL) {
  label <- if (is.null(expr)) NULL else rlang::expr_label(expr)
  message <- paste0(context, " does not support ", label %||% "this expression")
  if (!is.null(details)) {
    message <- paste(message, details)
  }
  cli::cli_abort(message, class = "mongo_tidy_unsupported")
}

#' @keywords internal
abort_invalid <- function(context, message) {
  cli::cli_abort(paste(context, message), class = "mongo_tidy_invalid")
}

#' @keywords internal
as_named_character <- function(x) {
  stats::setNames(as.character(x), names(x))
}

#' @keywords internal
is_scalar_literal <- function(x) {
  is.null(x) || (is.atomic(x) && length(x) == 1)
}

#' @keywords internal
field_reference <- function(name) {
  paste0("$", name)
}

#' @keywords internal
is_safe_output_name <- function(name) {
  is.character(name) &&
    length(name) == 1L &&
    nzchar(name) &&
    !grepl("\\.", name) &&
    !startsWith(name, "$")
}

#' @keywords internal
allocate_output_name <- function(used = character(), prefix = "__mdbplyr_col_") {
  i <- 1L
  repeat {
    candidate <- paste0(prefix, i)
    if (!candidate %in% used) {
      return(candidate)
    }
    i <- i + 1L
  }
}

#' @keywords internal
invert_name_map <- function(x) {
  stats::setNames(names(x), unname(x))
}

#' @keywords internal
format_mongo_datetime <- function(x) {
  format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
}

#' @keywords internal
as_mongo_literal <- function(x) {
  if (inherits(x, "POSIXt") || inherits(x, "Date")) {
    if (length(x) == 1L) {
      return(list(`$date` = format_mongo_datetime(x)))
    }
    return(lapply(as.list(x), as_mongo_literal))
  }

  if (is.list(x) && !is.data.frame(x)) {
    return(lapply(x, as_mongo_literal))
  }

  x
}

#' @keywords internal
pipeline_to_json <- function(pipeline, pretty = FALSE) {
  jsonlite::toJSON(
    pipeline,
    auto_unbox = TRUE,
    pretty = pretty,
    null = "null"
  )
}

#' @keywords internal
render_pipeline_json <- function(pipeline) {
  rendered <- pipeline_to_json(pipeline, pretty = TRUE)
  gsub(
    "\\{\\s*\"\\$date\"\\s*:\\s*\"([^\"]+)\"\\s*\\}",
    "ISODate(\"\\1\")",
    rendered,
    perl = TRUE
  )
}

#' @keywords internal
expr_text <- function(x) {
  if (rlang::is_quosure(x)) {
    x <- rlang::get_expr(x)
  }
  rlang::expr_label(x)
}
