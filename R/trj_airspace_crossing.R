#' series of functions to determin crossing points of a trajectory
#'
#' @param .airspace_sf a sf object with an airspace ID, i.e. AS_ID
#'
label_crossings <- function(.trj, .airspace_sf, ..., .debug = FALSE){
  # prepare trajectory
  df <- .trj |>
    dplyr::select(UID, TIME, LAT, LON, FL) |>
    dplyr::arrange(TIME) |>  # force sequencing, saw strange things
    dplyr::mutate(SEQ_ID = row_number()) |>
    cast_latlon_to_pts() |>
    sf::st_intersection(.airspace_sf) |>
    dplyr::arrange(TIME) |>
    # label point before and after change of ID
    dplyr::mutate(
      CROSSING =
        ( lag(AS_ID, default = first(AS_ID)) != AS_ID  ) |
        ( lead(AS_ID, default = last(AS_ID)) != AS_ID  )
      ) |>
    label_runs(CROSSING)
  return(df)
}

#' extract crossings
#' helper function pulling out labelled crossings
extract_labelled_crossings <- function(.df_labelled, ...){
  df <- .df_labelled |>
    dplyr::filter(CROSSING)
  return(df)
}

#' helper function to replicate rows and filter them, if not successive points
#' The following walks over all previous/next intersection points and creates
#' previous - next pairs
#'
duplicate_successive_pairs <- function(.df){
  seq_cross_points <- 1:nrow(.df)
  tmp <- seq_cross_points |>
    purrr::map_dfr(
      .f = ~ .df |>
        dplyr::filter(row_number() %in% c(.x, .x+1)) |>
        dplyr::mutate( CROSS = first(SEQ_ID) == last(SEQ_ID) - 1) |>
        dplyr::filter(CROSS)
    )
  # add counter for pairs
  tmp <- tmp |> dplyr::mutate(CROSS = rep(1:(nrow(tmp) %/% 2), each = 2))
}

#' cast prev/next point into linestring
cast_crossing_latlon_to_ls <- function(.xpairs){
  xpairs_ls <- .xpairs |>
    dplyr::group_by(CROSS) |> #----------- per crossing pair
    dplyr::mutate(PREV_SEQ = dplyr::first(SEQ_ID)) |>
    dplyr::group_by(CROSS, PREV_SEQ) |>
    dplyr::group_modify(     # turn successive points into linestring
      .f = ~ cast_latlon_to_ls(.x)
      ,.keep = TRUE    # keep grouping
    ) |>
    dplyr::ungroup() |>
    sf::st_as_sf()
  return(xpairs_ls)
}
