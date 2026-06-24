# Joins compile to MongoDB $lookup (same-database collections). The right-hand
# side must be a plain `tbl_mongo` (a collection reference with a known schema
# and no lazy operations) so that foreign keys and column collisions can be
# resolved from its schema. Mutating joins (inner/left) either flatten the match
# (default: $lookup + $unwind + $replaceRoot) or keep it as a nested array
# column (unnest = FALSE). Filtering joins (semi/anti) keep the left columns and
# filter rows by whether a match exists.

#' @keywords internal
join_check_rhs <- function(y, context) {
  if (!inherits(y, "tbl_mongo")) {
    abort_invalid(context, "`y` must be a tbl_mongo.")
  }
  if (length(compile_pipeline(y)) > 0) {
    abort_unsupported(
      context, NULL,
      "the right-hand table must be a plain collection reference; collect() it or drop its lazy operations first."
    )
  }
}

#' @keywords internal
normalize_join_by <- function(by, left_visible, right_visible, context) {
  if (inherits(by, "dplyr_join_by")) {
    abort_unsupported(context, NULL, "join_by() is not supported; pass a character `by` such as by = c(\"a\" = \"b\").")
  }
  if (is.null(by)) {
    common <- intersect(left_visible, right_visible)
    if (!length(common)) {
      abort_invalid(context, "no common columns to join by; specify `by`.")
    }
    return(list(left = common, right = common))
  }
  if (!is.character(by)) {
    abort_unsupported(context, NULL, "`by` must be a character vector, optionally named (by = c(\"a\" = \"b\")).")
  }

  nms <- names(by)
  left_keys <- if (is.null(nms)) unname(by) else ifelse(nzchar(nms), nms, unname(by))
  right_keys <- unname(by)

  missing_left <- setdiff(left_keys, left_visible)
  if (length(missing_left)) {
    abort_invalid(context, paste0("unknown left join column(s): ", paste(missing_left, collapse = ", "), "."))
  }
  missing_right <- setdiff(right_keys, right_visible)
  if (length(missing_right)) {
    abort_invalid(context, paste0("unknown right join column(s): ", paste(missing_right, collapse = ", "), "."))
  }

  list(left = left_keys, right = right_keys)
}

#' @keywords internal
build_lookup_stage <- function(from, left_key_actual, right_key_actual, as_field) {
  let_names <- paste0("mdb_l", seq_along(left_key_actual) - 1L)
  let_vars <- stats::setNames(as.list(field_reference(left_key_actual)), let_names)
  conds <- lapply(seq_along(right_key_actual), function(i) {
    list(`$eq` = list(field_reference(right_key_actual[[i]]), paste0("$$", let_names[[i]])))
  })
  match_expr <- if (length(conds) == 1L) conds[[1]] else list(`$and` = conds)

  list(`$lookup` = list(
    from = from,
    let = let_vars,
    pipeline = list(list(`$match` = list(`$expr` = match_expr))),
    as = as_field
  ))
}

#' @keywords internal
build_join_merge <- function(left_map, right_map, by_spec, suffix, as_field) {
  left_visible <- names(left_map)
  right_visible <- names(right_map)
  collide <- intersect(
    setdiff(left_visible, by_spec$left),
    setdiff(right_visible, by_spec$right)
  )

  specs <- list()
  for (lv in left_visible) {
    visible <- if (lv %in% collide) paste0(lv, suffix[[1]]) else lv
    specs[[length(specs) + 1L]] <- list(visible = visible, expr = field_reference(unname(left_map[[lv]])))
  }
  for (rv in setdiff(right_visible, by_spec$right)) {
    visible <- if (rv %in% collide) paste0(rv, suffix[[2]]) else rv
    specs[[length(specs) + 1L]] <- list(
      visible = visible,
      expr = field_reference(paste0(as_field, ".", unname(right_map[[rv]])))
    )
  }

  used <- character()
  field_map <- character()
  new_root <- list()
  for (spec in specs) {
    internal <- if (is_safe_output_name(spec$visible) && !spec$visible %in% used) {
      spec$visible
    } else {
      allocate_output_name(used)
    }
    used <- c(used, internal)
    new_root[[internal]] <- spec$expr
    field_map[[spec$visible]] <- internal
  }

  list(new_root = new_root, field_map = as_named_character(field_map))
}

#' @keywords internal
join_mutating <- function(x, y, by, suffix, type, unnest, name, keep, context) {
  join_check_rhs(y, context)
  if (isTRUE(keep)) {
    abort_unsupported(context, NULL, "keep = TRUE is not supported; join keys are kept from the left table only.")
  }
  if (!is.character(suffix) || length(suffix) != 2L) {
    abort_invalid(context, "`suffix` must be a character vector of length 2.")
  }

  left_map <- projection_mapping(x)
  right_map <- projection_mapping(y)
  if (!length(left_map) || !length(right_map)) {
    abort_invalid(context, "joins require known schemas on both tables. Supply schema or call infer_schema().")
  }

  by_spec <- normalize_join_by(by, names(left_map), names(right_map), context)
  left_key_actual <- resolve_field_sources(by_spec$left, left_map)
  right_key_actual <- resolve_field_sources(by_spec$right, right_map)
  from <- y$src$name %||% "collection"

  if (isTRUE(unnest)) {
    as_field <- "__mdbplyr_join__"
    lookup <- build_lookup_stage(from, left_key_actual, right_key_actual, as_field)
    unwind <- list(`$unwind` = c(
      list(path = field_reference(as_field)),
      if (identical(type, "left")) list(preserveNullAndEmptyArrays = TRUE) else NULL
    ))
    merge <- build_join_merge(left_map, right_map, by_spec, suffix, as_field)
    stages <- list(lookup, unwind, list(`$replaceRoot` = list(newRoot = merge$new_root)))
    new_field_map <- merge$field_map
  } else {
    nested_name <- name %||% from
    internal <- if (is_safe_output_name(nested_name) && !nested_name %in% unname(left_map)) {
      nested_name
    } else {
      allocate_output_name(unname(left_map))
    }
    stages <- list(build_lookup_stage(from, left_key_actual, right_key_actual, internal))
    new_field_map <- as_named_character(c(left_map, stats::setNames(internal, nested_name)))
  }

  update_ir(
    x,
    field_map = new_field_map,
    collect_map = new_field_map,
    projection = NULL,
    ops = c(x$ir$ops, list(list(type = "join", stages = stages)))
  )
}

#' @keywords internal
join_filtering <- function(x, y, by, type, context) {
  join_check_rhs(y, context)
  left_map <- projection_mapping(x)
  right_map <- projection_mapping(y)
  if (!length(left_map) || !length(right_map)) {
    abort_invalid(context, "joins require known schemas on both tables. Supply schema or call infer_schema().")
  }

  by_spec <- normalize_join_by(by, names(left_map), names(right_map), context)
  left_key_actual <- resolve_field_sources(by_spec$left, left_map)
  right_key_actual <- resolve_field_sources(by_spec$right, right_map)
  from <- y$src$name %||% "collection"
  as_field <- "__mdbplyr_join_match__"

  lookup <- build_lookup_stage(from, left_key_actual, right_key_actual, as_field)
  match_op <- if (identical(type, "semi")) "$gt" else "$eq"
  match_stage <- list(`$match` = list(`$expr` = stats::setNames(
    list(list(list(`$size` = field_reference(as_field)), 0L)),
    match_op
  )))
  drop_stage <- list(`$project` = stats::setNames(list(0L), as_field))

  update_ir(
    x,
    ops = c(x$ir$ops, list(list(type = "join", stages = list(lookup, match_stage, drop_stage))))
  )
}

#' Join a lazy Mongo query to another collection
#'
#' @param x A `tbl_mongo` object (the left table).
#' @param y A plain `tbl_mongo` object (the right table): a collection reference
#'   with a known schema and no lazy operations.
#' @param by A character vector of shared columns, optionally named
#'   (`by = c("a" = "b")` joins `x$a` to `y$b`). When `NULL`, the common columns
#'   are used.
#' @param copy Ignored; included for `dplyr` compatibility.
#' @param suffix Length-2 character vector of suffixes for columns that collide
#'   between `x` and `y`.
#' @param ... Unused.
#' @param keep Only `NULL`/`FALSE` is supported; join keys come from the left
#'   table.
#' @param unnest When `TRUE` (default), the match is flattened to one row per
#'   matched pair (`$lookup` + `$unwind`). When `FALSE`, matches are kept as a
#'   nested array column.
#' @param name Name of the nested array column when `unnest = FALSE`. Defaults to
#'   the right collection name.
#'
#' @details
#' Joins compile to MongoDB `$lookup` against a collection in the same database.
#' `inner_join()` keeps only matched rows; `left_join()` keeps all left rows.
#'
#' @return A modified `tbl_mongo` object.
#' @rdname mongo_joins
#' @export
inner_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ..., keep = NULL, unnest = TRUE, name = NULL) {
  join_mutating(x, y, by, suffix, type = "inner", unnest = unnest, name = name, keep = keep, context = "inner_join()")
}

#' @rdname mongo_joins
#' @export
left_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE, suffix = c(".x", ".y"), ..., keep = NULL, unnest = TRUE, name = NULL) {
  join_mutating(x, y, by, suffix, type = "left", unnest = unnest, name = name, keep = keep, context = "left_join()")
}

#' @rdname mongo_joins
#' @export
semi_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE, ...) {
  join_filtering(x, y, by, type = "semi", context = "semi_join()")
}

#' @rdname mongo_joins
#' @export
anti_join.tbl_mongo <- function(x, y, by = NULL, copy = FALSE, ...) {
  join_filtering(x, y, by, type = "anti", context = "anti_join()")
}
