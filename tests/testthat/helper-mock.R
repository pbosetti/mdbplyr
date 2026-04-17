mock_collection <- function(data, name = "mock_collection") {
  executor <- function(pipeline, ...) {
    run_pipeline(data, pipeline)
  }

  structure(
    list(name = name, data = tibble::as_tibble(data), aggregate = NULL),
    class = "mock_mongo"
  ) -> collection

  collection$aggregate <- function(pipeline_json, iterate = FALSE, ...) {
    pipeline <- jsonlite::fromJSON(pipeline_json, simplifyVector = FALSE)
    result <- run_pipeline(data, pipeline)
    if (isTRUE(iterate)) {
      return(mock_cursor(result))
    }
    result
  }

  collection
}

mock_tbl <- function(data, name = "mock_collection") {
  collection <- mock_collection(data, name = name)
  tbl_mongo(collection, schema = names(data), executor = function(pipeline, ...) run_pipeline(data, pipeline))
}

#' Create a mock multi-collection "database" for join tests.
#'
#' Returns a named list of `tbl_mongo` objects whose executors share a common
#' in-memory table context so that `$lookup` stages work correctly.
#'
#' @param ... Named data frames, one per collection.
#' @return Named list of `tbl_mongo` objects.
#' @keywords internal
mock_db <- function(...) {
  table_data <- lapply(list(...), tibble::as_tibble)

  make_tbl <- function(name, data, all_tables) {
    force(name)
    force(data)
    force(all_tables)
    executor <- function(pipeline, ...) {
      run_pipeline(data, pipeline, all_tables)
    }
    collection <- structure(
      list(
        name = name,
        data = data,
        aggregate = (function(d, db) {
          function(pipeline_json, iterate = FALSE, ...) {
            pipeline <- jsonlite::fromJSON(pipeline_json, simplifyVector = FALSE)
            run_pipeline(d, pipeline, db)
          }
        })(data, all_tables)
      ),
      class = "mock_mongo"
    )
    tbl_mongo(collection, schema = names(data), executor = executor)
  }

  result <- lapply(names(table_data), function(n) {
    make_tbl(n, table_data[[n]], table_data)
  })
  stats::setNames(result, names(table_data))
}


mock_cursor <- function(data) {
  rows <- tibble::as_tibble(data)
  self <- local({
    offset <- 1L

    next_index <- function(size) {
      if (offset > nrow(rows)) {
        return(integer())
      }
      start <- offset
      stop <- min(nrow(rows), offset + size - 1L)
      offset <<- stop + 1L
      seq.int(start, stop)
    }

    one <- function() {
      idx <- next_index(1L)
      if (!length(idx)) {
        return(NULL)
      }
      as.list(rows[idx, , drop = FALSE])
    }

    batch <- function(size = 1000) {
      idx <- next_index(size)
      if (!length(idx)) {
        return(list())
      }
      lapply(idx, function(i) as.list(rows[i, , drop = FALSE]))
    }

    json <- function(size = 1000) {
      idx <- next_index(size)
      if (!length(idx)) {
        return(character())
      }
      vapply(
        idx,
        function(i) jsonlite::toJSON(rows[i, , drop = FALSE], auto_unbox = TRUE, null = "null"),
        character(1)
      )
    }

    page <- function(size = 1000) {
      idx <- next_index(size)
      if (!length(idx)) {
        return(tibble::as_tibble(rows[0, , drop = FALSE]))
      }
      tibble::as_tibble(rows[idx, , drop = FALSE])
    }

    environment()
  })

  lockEnvironment(self, FALSE)
  structure(self, class = c("mongo_iter", "jeroen", class(self)))
}

run_pipeline <- function(data, pipeline, db = NULL) {
  result <- tibble::as_tibble(data)
  for (stage in pipeline) {
    op <- names(stage)[[1]]
    spec <- stage[[1]]
    result <- switch(
      op,
      `$match` = apply_match(result, spec),
      `$addFields` = apply_add_fields(result, spec),
      `$project` = apply_project(result, spec),
      `$group` = apply_group(result, spec),
      `$sort` = apply_sort(result, spec),
      `$limit` = tibble::as_tibble(utils::head(result, spec)),
      `$lookup` = apply_lookup(result, spec, db),
      `$unwind` = apply_unwind(result, spec),
      `$replaceRoot` = apply_replace_root(result, spec),
      stop("Unsupported stage in test executor: ", op, call. = FALSE)
    )
  }
  tibble::as_tibble(result)
}

apply_match <- function(data, spec) {
  # The existing code used $expr only; handle both $expr and plain query operators.
  if (!is.null(spec$`$expr`)) {
    mask <- eval_expr(spec$`$expr`, data)
    return(tibble::as_tibble(data[isTRUE(mask) | mask %in% TRUE, , drop = FALSE]))
  }

  # Query-style: { field: { $op: value } }
  mask <- rep(TRUE, nrow(data))
  for (field in names(spec)) {
    condition <- spec[[field]]
    col <- data[[field]]
    field_mask <- if (is.list(condition) && length(condition) == 1L) {
      op  <- names(condition)[[1L]]
      val <- condition[[1L]]
      switch(
        op,
        `$ne` = vapply(col, function(v) !identical(v, val), logical(1L)),
        `$eq` = vapply(col, function(v)  identical(v, val), logical(1L)),
        `$size` = vapply(col, function(v) {
          n <- if (is.null(v)) 0L else if (is.data.frame(v)) nrow(v) else length(v)
          identical(n, as.integer(val))
        }, logical(1L)),
        stop("Unsupported query operator in test mock: ", op, call. = FALSE)
      )
    } else {
      # Plain equality: { field: value }
      vapply(col, function(v) identical(v, condition), logical(1L))
    }
    mask <- mask & field_mask
  }
  tibble::as_tibble(data[mask, , drop = FALSE])
}

apply_add_fields <- function(data, spec) {
  out <- tibble::as_tibble(data)
  for (name in names(spec)) {
    out[[name]] <- eval_expr(spec[[name]], out)
  }
  out
}

apply_project <- function(data, spec) {
  non_id <- spec[names(spec) != "_id"]

  # Exclusion mode: all non-_id values are 0L → drop those fields.
  if (length(non_id) > 0L && all(vapply(non_id, identical, logical(1L), 0L))) {
    keep <- setdiff(names(data), names(non_id))
    return(tibble::as_tibble(data[, keep, drop = FALSE]))
  }

  # Inclusion mode (original behaviour).
  out <- list()
  for (name in names(spec)) {
    expr <- spec[[name]]
    if (identical(name, "_id") && identical(expr, 0L)) {
      next
    }
    out[[name]] <- if (identical(expr, 1L)) {
      data[[name]]
    } else {
      eval_expr(expr, data)
    }
  }
  tibble::as_tibble(out)
}

apply_group <- function(data, spec) {
  summaries <- names(spec)[names(spec) != "_id"]
  if (is.null(spec$`_id`)) {
    groups <- list(all = seq_len(nrow(data)))
    keys <- list(NULL)
  } else {
    key_df <- as.data.frame(lapply(spec$`_id`, eval_expr, data = data), stringsAsFactors = FALSE)
    group_index <- split(seq_len(nrow(data)), interaction(key_df, drop = TRUE, lex.order = TRUE))
    groups <- unname(group_index)
    keys <- lapply(groups, function(idx) {
      as.list(key_df[idx[1], , drop = FALSE])
    })
  }

  rows <- vector("list", length(groups))
  for (i in seq_along(groups)) {
    idx <- groups[[i]]
    subset <- data[idx, , drop = FALSE]
    row <- list(`_id` = keys[[i]])
    for (name in summaries) {
      row[[name]] <- eval_agg(spec[[name]], subset)
    }
    rows[[i]] <- row
  }
  tibble::as_tibble(rows)
}

apply_sort <- function(data, spec) {
  order_vecs <- lapply(names(spec), function(name) {
    value <- data[[name]]
    if (identical(spec[[name]], -1L)) dplyr::desc(value) else value
  })
  tibble::as_tibble(data[do.call(order, order_vecs), , drop = FALSE])
}

eval_expr <- function(expr, data) {
  if (is.character(expr) && length(expr) == 1 && startsWith(expr, "$")) {
    return(resolve_field(data, substring(expr, 2)))
  }
  if (!is.list(expr) || is.data.frame(expr)) {
    return(rep(expr, nrow(data)))
  }

  op <- names(expr)[[1]]
  args <- expr[[1]]

  switch(
    op,
    `$eq` = compare_expr(eval_expr(args[[1]], data), eval_expr(args[[2]], data), function(a, b) a == b),
    `$ne` = compare_expr(eval_expr(args[[1]], data), eval_expr(args[[2]], data), function(a, b) a != b),
    `$gt` = eval_expr(args[[1]], data) > eval_expr(args[[2]], data),
    `$gte` = eval_expr(args[[1]], data) >= eval_expr(args[[2]], data),
    `$lt` = eval_expr(args[[1]], data) < eval_expr(args[[2]], data),
    `$lte` = eval_expr(args[[1]], data) <= eval_expr(args[[2]], data),
    `$and` = Reduce(`&`, lapply(args, eval_expr, data = data)),
    `$or` = Reduce(`|`, lapply(args, eval_expr, data = data)),
    `$not` = !eval_expr(args[[1]], data),
    `$add` = Reduce(`+`, lapply(args, eval_expr, data = data)),
    `$subtract` = eval_expr(args[[1]], data) - eval_expr(args[[2]], data),
    `$multiply` = Reduce(`*`, lapply(args, eval_expr, data = data)),
    `$divide` = eval_expr(args[[1]], data) / eval_expr(args[[2]], data),
    `$pow` = eval_expr(args[[1]], data) ^ eval_expr(args[[2]], data),
    `$mod` = eval_expr(args[[1]], data) %% eval_expr(args[[2]], data),
    `$floor` = floor(eval_expr(args[[1]], data)),
    `$ceil` = ceiling(eval_expr(args[[1]], data)),
    `$trunc` = eval_trunc(args, data),
    `$log` = log(eval_expr(args[[1]], data), eval_expr(args[[2]], data)),
    `$log10` = log10(eval_expr(args[[1]], data)),
    `$abs` = abs(eval_expr(args[[1]], data)),
    `$sqrt` = sqrt(eval_expr(args[[1]], data)),
    `$ln` = log(eval_expr(args[[1]], data)),
    `$exp` = exp(eval_expr(args[[1]], data)),
    `$sin` = sin(eval_expr(args[[1]], data)),
    `$cos` = cos(eval_expr(args[[1]], data)),
    `$tan` = tan(eval_expr(args[[1]], data)),
    `$asin` = asin(eval_expr(args[[1]], data)),
    `$acos` = acos(eval_expr(args[[1]], data)),
    `$atan` = atan(eval_expr(args[[1]], data)),
    `$atan2` = atan2(eval_expr(args[[1]], data), eval_expr(args[[2]], data)),
    `$sinh` = sinh(eval_expr(args[[1]], data)),
    `$cosh` = cosh(eval_expr(args[[1]], data)),
    `$tanh` = tanh(eval_expr(args[[1]], data)),
    `$asinh` = asinh(eval_expr(args[[1]], data)),
    `$acosh` = acosh(eval_expr(args[[1]], data)),
    `$atanh` = atanh(eval_expr(args[[1]], data)),
    `$min` = Reduce(pmin, lapply(args, eval_expr, data = data)),
    `$max` = Reduce(pmax, lapply(args, eval_expr, data = data)),
    `$concat` = eval_concat(args, data),
    `$toLower` = tolower(eval_expr(args[[1]], data)),
    `$toUpper` = toupper(eval_expr(args[[1]], data)),
    `$strLenCP` = nchar(eval_expr(args[[1]], data), type = "chars", allowNA = TRUE, keepNA = TRUE),
    `$substrCP` = eval_substr_cp(args, data),
    `$in` = eval_in(args, data),
    `$round` = round(eval_expr(args[[1]], data), args[[2]]),
    `$cond` = ifelse(eval_expr(args$`if`, data), eval_expr(args$then, data), eval_expr(args$`else`, data)),
    `$switch` = eval_switch(args, data),
    stop("Unsupported expression in test executor: ", op, call. = FALSE)
  )
}

compare_expr <- function(lhs, rhs, comparator) {
  if (is.null(rhs)) {
    return(is.na(lhs))
  }
  if (is.null(lhs)) {
    return(is.na(rhs))
  }
  comparator(lhs, rhs)
}

eval_switch <- function(spec, data) {
  result <- eval_expr(spec$default, data)
  for (branch in rev(spec$branches)) {
    mask <- eval_expr(branch$case, data)
    branch_value <- eval_expr(branch$then, data)
    result <- ifelse(mask, branch_value, result)
  }
  result
}

eval_trunc <- function(args, data) {
  values <- eval_expr(args[[1]], data)
  if (length(args) == 1) {
    return(trunc(values))
  }

  digits <- eval_expr(args[[2]], data)
  scale <- 10 ^ digits
  trunc(values * scale) / scale
}

eval_concat <- function(args, data) {
  parts <- lapply(args, eval_expr, data = data)
  do.call(paste0, parts)
}

eval_substr_cp <- function(args, data) {
  text <- eval_expr(args[[1]], data)
  start <- eval_expr(args[[2]], data)
  count <- eval_expr(args[[3]], data)
  mapply(
    function(value, first, width) {
      substring(value, first = first + 1, last = first + width)
    },
    text,
    start,
    count,
    USE.NAMES = FALSE
  )
}

eval_in <- function(args, data) {
  lhs <- eval_expr(args[[1]], data)
  rhs_spec <- args[[2]]
  rhs <- if (!is.list(rhs_spec) && length(rhs_spec) > 1) {
    replicate(nrow(data), rhs_spec, simplify = FALSE)
  } else {
    eval_expr(rhs_spec, data)
  }

  if (!is.list(rhs)) {
    rhs <- replicate(length(lhs), rhs, simplify = FALSE)
  }

  mapply(function(value, options) value %in% options, lhs, rhs, USE.NAMES = FALSE)
}

resolve_field <- function(data, path) {
  if (path %in% names(data)) {
    return(data[[path]])
  }

  parts <- strsplit(path, ".", fixed = TRUE)[[1]]
  value <- data[[parts[[1]]]]
  if (length(parts) == 1) {
    return(value)
  }

  for (part in parts[-1]) {
    value <- sapply(value, function(item) {
      if (is.list(item) && !is.null(item[[part]])) item[[part]] else NA
    }, simplify = TRUE, USE.NAMES = FALSE)
  }
  value
}

eval_agg <- function(expr, data) {
  op <- names(expr)[[1]]
  arg <- expr[[1]]
  values <- if (identical(op, "$sum") && identical(arg, 1L)) {
    rep(1L, nrow(data))
  } else {
    eval_expr(arg, data)
  }

  switch(
    op,
    `$sum` = sum(values, na.rm = TRUE),
    `$avg` = mean(values, na.rm = TRUE),
    `$min` = min(values, na.rm = TRUE),
    `$max` = max(values, na.rm = TRUE),
    stop("Unsupported aggregate in test executor: ", op, call. = FALSE)
  )
}

# ---- join stage helpers -----------------------------------------------------

# Recursively replace $$var references with their resolved scalar values.
substitute_let_vars <- function(expr, let_vars) {
  if (is.character(expr) && length(expr) == 1L && startsWith(expr, "$$")) {
    var_name <- substring(expr, 3L)
    val <- let_vars[[var_name]]
    if (!is.null(val)) return(val)
  }
  if (is.list(expr)) {
    return(lapply(expr, substitute_let_vars, let_vars = let_vars))
  }
  expr
}

apply_lookup <- function(data, spec, db) {
  if (is.null(db)) {
    stop("$lookup requires a database context; use mock_db() instead of mock_tbl().", call. = FALSE)
  }
  from_data <- db[[spec$from]]
  if (is.null(from_data)) {
    stop("$lookup: unknown collection '", spec$from, "'", call. = FALSE)
  }
  from_data <- tibble::as_tibble(from_data)
  joined_col <- spec$as
  n <- nrow(data)
  results <- vector("list", n)

  for (i in seq_len(n)) {
    row <- data[i, , drop = FALSE]

    # Resolve each let variable to a scalar for this row.
    let_vals <- lapply(spec$let, function(expr_str) {
      v <- eval_expr(expr_str, row)
      if (length(v) >= 1L) v[[1L]] else v
    })

    # Substitute $$var references so eval_expr never sees them.
    row_pipeline <- substitute_let_vars(spec$pipeline, let_vals)

    results[[i]] <- run_pipeline(from_data, row_pipeline, db)
  }

  data[[joined_col]] <- results
  tibble::as_tibble(data)
}

apply_unwind <- function(data, spec) {
  if (is.character(spec)) {
    path    <- spec
    preserve <- FALSE
  } else {
    path    <- spec$path
    preserve <- isTRUE(spec$preserveNullAndEmptyArrays)
  }
  field_name <- if (startsWith(path, "$")) substring(path, 2L) else path

  col <- data[[field_name]]
  other_cols <- setdiff(names(data), field_name)

  result_rows <- list()
  for (i in seq_len(nrow(data))) {
    arr <- col[[i]]
    base <- as.list(data[i, other_cols, drop = FALSE])

    is_empty <- is.null(arr) ||
      (is.data.frame(arr) && nrow(arr) == 0L) ||
      (is.list(arr) && !is.data.frame(arr) && length(arr) == 0L)

    if (is_empty) {
      if (preserve) {
        base[[field_name]] <- list(NULL)
        result_rows[[length(result_rows) + 1L]] <- tibble::as_tibble(base)
      }
      next
    }

    if (is.data.frame(arr)) {
      for (j in seq_len(nrow(arr))) {
        new_row <- base
        new_row[[field_name]] <- list(as.list(arr[j, , drop = FALSE]))
        result_rows[[length(result_rows) + 1L]] <- tibble::as_tibble(new_row)
      }
    } else if (is.list(arr)) {
      for (item in arr) {
        new_row <- base
        new_row[[field_name]] <- list(item)
        result_rows[[length(result_rows) + 1L]] <- tibble::as_tibble(new_row)
      }
    }
  }

  if (length(result_rows) == 0L) {
    return(data[0L, , drop = FALSE])
  }
  dplyr::bind_rows(result_rows)
}

apply_replace_root <- function(data, spec) {
  new_root_spec <- spec$newRoot
  if (!is.list(new_root_spec) || !("$mergeObjects" %in% names(new_root_spec))) {
    stop("$replaceRoot: only $mergeObjects is supported in the test mock.", call. = FALSE)
  }

  merge_args <- new_root_spec$`$mergeObjects`

  result_rows <- lapply(seq_len(nrow(data)), function(i) {
    row <- data[i, , drop = FALSE]
    merged <- list()

    for (arg in merge_args) {
      contribution <- if (identical(arg, "$$ROOT")) {
        # Expand the current row into a named list. List-columns whose
        # single element is itself a list (embedded document) must stay
        # wrapped so that tibble::as_tibble sees a length-1 list-column,
        # not a length-N nested list.
        out <- lapply(names(row), function(n) {
          v <- row[[n]]
          if (is.list(v) && length(v) == 1L) {
            inner <- v[[1L]]
            if (is.list(inner)) v else inner  # keep embedded docs wrapped
          } else {
            v
          }
        })
        stats::setNames(out, names(row))
      } else if (is.character(arg) && length(arg) == 1L && startsWith(arg, "$")) {
        # Reference to a field: extract and spread its named-list value.
        field_name <- substring(arg, 2L)
        v <- if (field_name %in% names(row)) row[[field_name]] else NULL
        # Unwrap list-column wrapper if present.
        if (is.list(v) && length(v) == 1L) v <- v[[1L]]
        if (is.list(v)) v else NULL
      } else if (is.list(arg) && "$ifNull" %in% names(arg)) {
        # $ifNull: [field_ref, default] – used for left joins.
        ifnull_args <- arg$`$ifNull`
        field_name  <- substring(ifnull_args[[1L]], 2L)  # strip "$"
        v <- if (field_name %in% names(row)) row[[field_name]] else NULL
        if (is.list(v) && length(v) == 1L) v <- v[[1L]]
        if (is.null(v) || (is.list(v) && length(v) == 0L)) {
          default <- ifnull_args[[2L]]
          if (is.list(default)) default else list()
        } else if (is.list(v)) {
          v
        } else {
          NULL
        }
      } else {
        NULL
      }

      if (!is.null(contribution) && is.list(contribution)) {
        merged <- utils::modifyList(merged, contribution)
      }
    }

    tibble::as_tibble(merged)
  })

  dplyr::bind_rows(result_rows)
}
