# ---- helpers ----------------------------------------------------------------

#' @keywords internal
parse_join_by <- function(by, x, y) {
  if (is.null(by)) {
    x_schema <- x$ir$schema
    y_schema <- y$ir$schema
    if (!length(x_schema) || !length(y_schema)) {
      abort_invalid(
        "join()",
        "cannot infer `by` when schema is unknown. Supply `by` or add schema to both tables."
      )
    }
    common <- intersect(x_schema, y_schema)
    if (!length(common)) {
      abort_invalid("join()", "no common columns found. Specify `by` explicitly.")
    }
    list(local_keys = common, foreign_keys = common)
  } else if (is.character(by)) {
    nms <- names(by) %||% rep("", length(by))
    local_keys  <- ifelse(nzchar(nms), nms, by)
    foreign_keys <- unname(by)
    list(local_keys = local_keys, foreign_keys = foreign_keys)
  } else {
    abort_invalid("join()", "`by` must be NULL or a character vector.")
  }
}

#' @keywords internal
build_join_tbl <- function(x, y, by, join_type) {
  if (!inherits(y, "tbl_mongo")) {
    abort_unsupported(
      paste0(join_type, "_join()"),
      NULL,
      paste0(
        "y must be a tbl_mongo. ",
        "Joining against a plain data frame is not supported; ",
        "wrap it in tbl_mongo() first."
      )
    )
  }

  keys <- parse_join_by(by, x, y)

  join_info <- list(
    type         = join_type,
    from         = y$src$name,
    local_keys   = keys$local_keys,
    foreign_keys = keys$foreign_keys,
    y_pipeline   = compile_pipeline(y),
    y_schema     = y$ir$schema
  )

  # For semi/anti the result schema is x's schema only; for other joins
  # bring in y's non-key fields (those not used as the foreign match key).
  new_schema <- if (join_type %in% c("semi", "anti")) {
    x$ir$schema
  } else {
    y_extra <- setdiff(join_info$y_schema, join_info$foreign_keys)
    unique(c(x$ir$schema, y_extra))
  }

  update_ir(x, join = join_info, schema = new_schema)
}

# ---- join verbs -------------------------------------------------------------

#' Join two lazy MongoDB tables
#'
#' These functions implement the dplyr join verbs for `tbl_mongo` objects.
#' They compile to MongoDB `$lookup` aggregation stages, so **both** `x` and
#' `y` must be `tbl_mongo` objects backed by collections in the **same**
#' MongoDB database.
#'
#' The join is lazy: no data is fetched until [collect()] is called. The
#' compiled aggregation pipeline can be inspected with [show_query()].
#'
#' Only `inner_join`, `left_join`, `semi_join`, and `anti_join` are supported
#' natively via `$lookup`. `right_join` and `full_join` are not supported
#' because MongoDB has no native equivalent that can be expressed as a single
#' aggregation starting from the left collection.
#'
#' @param x A `tbl_mongo` object (left table).
#' @param y A `tbl_mongo` object (right table, same database as `x`).
#' @param by A character vector of column names to join on.  Use a *named*
#'   vector to join on columns with different names:
#'   `by = c("left_col" = "right_col")`.  When `NULL` (default), the join
#'   uses all columns that appear in both `x` and `y` schemas.
#' @param copy Ignored (included for dplyr compatibility).
#' @param suffix Ignored (column-name conflicts are not yet handled
#'   automatically; rename columns beforehand if needed).
#' @param keep Ignored (included for dplyr compatibility).
#' @param ... Ignored.
#'
#' @return A modified `tbl_mongo` object with the join recorded in the IR.
#'   The result schema is the union of `x`'s and `y`'s schemas (minus the
#'   duplicate foreign key when the join keys have different names).
#'
#' @examples
#' orders <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("order_id", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#' customers <- tbl_mongo(
#'   list(name = "customers"),
#'   schema = c("order_id", "name"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' q <- dplyr::inner_join(orders, customers, by = "order_id")
#' show_query(q)
#' @rdname mongo_join
#' @export
inner_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE,
                                  suffix = c(".x", ".y"), ..., keep = NULL) {
  build_join_tbl(x, y, by, "inner")
}

#' @rdname mongo_join
#' @export
left_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE,
                                 suffix = c(".x", ".y"), ..., keep = NULL) {
  build_join_tbl(x, y, by, "left")
}

#' @rdname mongo_join
#' @export
right_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE,
                                  suffix = c(".x", ".y"), ..., keep = NULL) {
  abort_unsupported(
    "right_join()",
    NULL,
    paste0(
      "right_join() is not supported for tbl_mongo because MongoDB aggregation ",
      "pipelines cannot start from the right collection. ",
      "Swap x and y and use left_join() instead."
    )
  )
}

#' @rdname mongo_join
#' @export
full_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE,
                                 suffix = c(".x", ".y"), ..., keep = NULL,
                                 na_matches = c("na", "never")) {
  abort_unsupported(
    "full_join()",
    NULL,
    paste0(
      "full_join() is not supported for tbl_mongo. ",
      "MongoDB does not have a native full outer join stage."
    )
  )
}

#' @rdname mongo_join
#' @export
semi_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE, ...) {
  build_join_tbl(x, y, by, "semi")
}

#' @rdname mongo_join
#' @export
anti_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE, ...) {
  build_join_tbl(x, y, by, "anti")
}
