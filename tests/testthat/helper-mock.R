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

run_pipeline <- function(data, pipeline) {
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
      stop("Unsupported stage in test executor: ", op, call. = FALSE)
    )
  }
  tibble::as_tibble(result)
}

apply_match <- function(data, spec) {
  mask <- eval_expr(spec$`$expr`, data)
  tibble::as_tibble(data[isTRUE(mask) | mask %in% TRUE, , drop = FALSE])
}

apply_add_fields <- function(data, spec) {
  out <- tibble::as_tibble(data)
  for (name in names(spec)) {
    out[[name]] <- eval_expr(spec[[name]], out)
  }
  out
}

apply_project <- function(data, spec) {
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
