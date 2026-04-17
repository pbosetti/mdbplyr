## ----setup, include = FALSE---------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----load-starwars, eval = FALSE----------------------------------------------
# library(dplyr)
# library(mongolite)
# library(mdbplyr)
# 
# starwars_collection <- mongolite::mongo(
#   collection = "starwars",
#   db = "mdbplyr"
# )
# 
# starwars_collection$drop()
# starwars_collection$insert(dplyr::starwars)
# 
# starwars_tbl <- tbl_mongo(
#   starwars_collection,
#   schema = names(dplyr::starwars)
# )

## ----create-lazy-table, eval = FALSE------------------------------------------
# library(dplyr)
# library(mdbplyr)
# 
# starwars_collection <- mongolite::mongo(
#   collection = "starwars",
#   db = "mdbplyr"
# )
# 
# starwars_tbl <- tbl_mongo(
#   starwars_collection,
#   schema = names(dplyr::starwars)
# )

## ----inspect, eval = FALSE----------------------------------------------------
# schema_fields(starwars_tbl)
# 
# starwars_tbl |>
#   filter(species == "Human", height > 180) |>
#   select(name, height, mass) |>
#   show_query()

## ----inspect-cursor, eval = FALSE---------------------------------------------
# iter <- starwars_tbl |>
#   filter(species == "Human", height > 180) |>
#   select(name, height, mass) |>
#   cursor()
# 
# iter$page(10)

## ----verb-filter, eval = FALSE------------------------------------------------
# starwars_tbl |>
#   filter(species == "Droid", height > 100) |>
#   collect()

## ----verb-select, eval = FALSE------------------------------------------------
# starwars_tbl |>
#   select(name, species, homeworld) |>
#   collect()

## ----verb-rename, eval = FALSE------------------------------------------------
# starwars_tbl |>
#   rename(character_name = name, planet = homeworld) |>
#   collect()

## ----verb-mutate, eval = FALSE------------------------------------------------
# starwars_tbl |>
#   mutate(height_m = height / 100, bmi_like = mass / (height_m * height_m)) |>
#   select(name, height, mass, height_m, bmi_like) |>
#   collect()

## ----verb-transmute, eval = FALSE---------------------------------------------
# starwars_tbl |>
#   transmute(name = name, height_m = height / 100) |>
#   collect()

## ----verb-arrange, eval = FALSE-----------------------------------------------
# starwars_tbl |>
#   arrange(desc(height), name) |>
#   select(name, height) |>
#   slice_head(n = 10) |>
#   collect()

## ----verb-group-by, eval = FALSE----------------------------------------------
# starwars_tbl |>
#   group_by(species)

## ----verb-summarise, eval = FALSE---------------------------------------------
# starwars_tbl |>
#   group_by(species) |>
#   summarise(
#     n = n(),
#     avg_height = mean(height),
#     max_mass = max(mass)
#   ) |>
#   arrange(desc(n)) |>
#   collect()

## ----verb-slice-head, eval = FALSE--------------------------------------------
# starwars_tbl |>
#   select(name, species) |>
#   slice_head(n = 5) |>
#   collect()

## ----verb-head, eval = FALSE--------------------------------------------------
# head(starwars_tbl, 5) |>
#   collect()

