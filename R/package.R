#' mdbplyr package
#'
#' A native tidy lazy backend for MongoDB aggregation pipelines.
#'
#' @keywords internal
"_PACKAGE"

#' @keywords internal
register_tbl_mongo_methods <- function() {
  dplyr_generics <- c(
    "anti_join",
    "arrange",
    "collect",
    "filter",
    "full_join",
    "group_by",
    "inner_join",
    "left_join",
    "mutate",
    "rename",
    "right_join",
    "select",
    "semi_join",
    "show_query",
    "slice_head",
    "summarise",
    "transmute"
  )

  dplyr_ns <- asNamespace("dplyr")
  for (generic in dplyr_generics) {
    if (exists(generic, envir = dplyr_ns, mode = "function", inherits = FALSE)) {
      registerS3method(
        generic,
        "tbl_mongo",
        get(paste0(generic, ".tbl_mongo"), envir = parent.env(environment())),
        envir = dplyr_ns
      )
    }
  }
}

#' @keywords internal
.onLoad <- function(libname, pkgname) {
  register_tbl_mongo_methods()
}
