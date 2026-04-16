#' Append a manual MongoDB aggregation stage
#'
#' Appends a single raw MongoDB aggregation stage, provided as a JSON string,
#' after the stages generated from the current lazy query.
#'
#' This is an escape hatch for features not yet modeled by the package. The
#' package does not attempt to infer schema changes introduced by manual stages.
#'
#' @param x A `tbl_mongo` object.
#' @param json_string A JSON string representing a single MongoDB pipeline
#'   stage, such as `{"$match":{"status":"paid"}}`.
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' query <- append_stage(tbl, "{\"$limit\": 1}")
#' show_query(query)
#' @export
append_stage <- function(x, json_string) {
  if (!inherits(x, "tbl_mongo")) {
    abort_invalid("append_stage()", "requires a tbl_mongo object.")
  }

  stage <- parse_manual_stage(json_string)
  update_ir(x, manual_stages = c(x$ir$manual_stages, list(stage)))
}

#' @keywords internal
parse_manual_stage <- function(json_string) {
  if (!is.character(json_string) || length(json_string) != 1 || is.na(json_string)) {
    abort_invalid("append_stage()", "requires a single JSON string.")
  }

  stage <- tryCatch(
    jsonlite::fromJSON(json_string, simplifyVector = FALSE),
    error = function(err) {
      abort_invalid("append_stage()", paste("invalid JSON stage:", conditionMessage(err)))
    }
  )

  if (!is.list(stage) || is.data.frame(stage) || length(stage) != 1) {
    abort_invalid(
      "append_stage()",
      "requires a single MongoDB stage object, for example a JSON string for one $match stage."
    )
  }

  stage_names <- names(stage)
  if (is.null(stage_names) || length(stage_names) != 1) {
    abort_invalid(
      "append_stage()",
      "requires a single MongoDB stage object, for example a JSON string for one $match stage."
    )
  }

  operator <- stage_names[[1]]
  if (!nzchar(operator) || !startsWith(operator, "$")) {
    abort_invalid("append_stage()", "stage object must have a single MongoDB operator key starting with '$'.")
  }

  stage
}
