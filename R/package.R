#' MongoTidy package
#'
#' A native tidy lazy backend for MongoDB aggregation pipelines.
#'
#' @keywords internal
"_PACKAGE"

#' @keywords internal
register_tbl_mongo_methods <- function() {
  dplyr_generics <- c(
    "arrange",
    "collect",
    "filter",
    "group_by",
    "mutate",
    "rename",
    "select",
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
