#' Group a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Bare field names or named computed keys such as
#'   `bucket = floor(amount / 10)`.
#' @param .add Whether to add to existing groups.
#' @param .drop Included for dplyr compatibility.
#'
#' @details
#' Bare field names group on the field directly. A computed grouping key must be
#' named (for example `group_by(decade = floor(year / 10) * 10)`); the expression
#' is translated into the MongoDB `$group._id` and follows the same field-vs-local
#' resolution rules as [filter.tbl_mongo()].
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' dplyr::group_by(tbl, status)
#' dplyr::group_by(tbl, bucket = floor(amount / 10))
#' @rdname mongo_group_by
#' @export
group_by.tbl_mongo <- function(.data, ..., .add = FALSE, .drop = dplyr::group_by_drop_default(.data)) {
  quos <- rlang::enquos(...)
  current_map <- projection_mapping(.data)
  names_in <- rlang::names2(quos)

  groups <- character()
  defs <- list()
  for (i in seq_along(quos)) {
    expr <- rlang::get_expr(quos[[i]])
    nm <- names_in[[i]]

    if (rlang::is_symbol(expr)) {
      src <- rlang::as_string(expr)
      out <- if (nzchar(nm)) nm else src
      defs[[out]] <- list(type = "field", source = src)
    } else {
      if (!nzchar(nm)) {
        abort_unsupported(
          "group_by()", expr,
          "Computed group keys must be named, e.g. group_by(bucket = floor(amount / 10))."
        )
      }
      out <- nm
      defs[[out]] <- list(
        type = "expr",
        expr = translate_expr(quos[[i]], context = "group_by()", field_map = current_map)
      )
    }
    groups <- c(groups, out)
  }

  if (isTRUE(.add)) {
    defs <- utils::modifyList(.data$ir$group_defs %||% list(), defs)
    groups <- unique(c(.data$ir$groups, groups))
  }

  update_ir(.data, groups = groups, group_defs = defs)
}

#' @keywords internal
finalize_group_defs <- function(groups, defs, current_map) {
  if (!length(groups)) {
    return(NULL)
  }

  stats::setNames(lapply(groups, function(group) {
    def <- if (!is.null(defs)) defs[[group]] else NULL
    if (!is.null(def) && identical(def$type, "expr")) {
      return(def)
    }
    source_visible <- if (!is.null(def) && !is.null(def$source)) def$source else group
    list(type = "field", source = resolve_field_sources(source_visible, current_map))
  }), groups)
}

#' @keywords internal
assert_no_computed_group_partition <- function(.data, context) {
  defs <- .data$ir$group_defs
  groups <- .data$ir$groups
  if (is.null(defs) || !length(groups)) {
    return(invisible())
  }

  computed <- groups[vapply(groups, function(group) identical(defs[[group]]$type, "expr"), logical(1))]
  if (length(computed)) {
    cli::cli_abort(
      paste0(
        context, " does not support row numbering with 1:n() after computed group_by() keys (",
        paste(computed, collapse = ", "), ")."
      ),
      class = "mongo_tidy_unsupported"
    )
  }

  invisible()
}

#' Summarise a lazy Mongo query
#'
#' @param .data A `tbl_mongo` object.
#' @param ... Named summary expressions.
#' @param .by Unsupported.
#' @param .groups Included for dplyr compatibility.
#'
#' @details
#' Summary expressions follow the same field-vs-local name resolution rules as
#' [filter.tbl_mongo()].
#'
#' @return A modified `tbl_mongo` object.
#' @examples
#' tbl <- tbl_mongo(
#'   list(name = "orders"),
#'   schema = c("status", "amount"),
#'   executor = function(pipeline, ...) tibble::tibble()
#' )
#'
#' query <- tbl |>
#'   dplyr::group_by(status) |>
#'   dplyr::summarise(total = sum(amount))
#'
#' show_query(query)
#' @rdname mongo_summarise
#' @export
summarise.tbl_mongo <- function(.data, ..., .by = NULL, .groups = NULL) {
  if (!is.null(.by)) {
    abort_unsupported("summarise()", .by, ".by is not supported.")
  }

  quos <- rlang::enquos(...)
  quos <- expand_across_quos(quos, names(projection_mapping(.data)), "summarise()")
  if (!length(quos)) {
    abort_invalid("summarise()", "requires at least one summary expression.")
  }

  names_in <- rlang::names2(quos)
  names_in[names_in == ""] <- vapply(quos[names_in == ""], expr_text, character(1))
  current_map <- projection_mapping(.data)
  translated <- lapply(quos, translate_agg, field_map = current_map)

  if (any(vapply(translated, function(agg) isTRUE(agg$fn %in% c("median", "quantile")), logical(1)))) {
    require_server_version(.data, "7.0", "summarise() with median() or quantile()")
  }

  shape <- append_field_map(character(), c(.data$ir$groups, names_in))
  group_output <- shape$field_map[.data$ir$groups]
  summary_output <- shape$field_map[names_in]
  names(translated) <- unname(summary_output)

  update_ir(
    .data,
    summaries = translated,
    projection = stats::setNames(unname(c(group_output, summary_output)), unname(c(group_output, summary_output))),
    field_map = as_named_character(c(group_output, summary_output)),
    collect_map = as_named_character(c(group_output, summary_output)),
    ops = c(.data$ir$ops, list(list(
      type = "summarise",
      groups = .data$ir$groups,
      group_defs = finalize_group_defs(.data$ir$groups, .data$ir$group_defs, current_map),
      summaries = translated
    )))
  )
}
