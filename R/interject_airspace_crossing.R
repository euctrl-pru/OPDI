inject_airspace_crossings <- function(.trj, .airspace_poly_sf){
  # Load aircraft trajectory data as an sf object, append airspace volumne, and
  # add sequence id
  trj <- .trj |> cast_latlon_to_pts() |>
    select(UID, TIME, LAT, LON, FL) |>
    dplyr::arrange(TIME) |>
    sf::st_intersection(.airspace_poly_sf) |>
    dplyr::arrange(TIME) |>      # saw some strange reshuffling after sf
    dplyr::mutate(SEQ_ID = dplyr::row_number())

  # calculate intersections - heuristic check for change in airspace valume
  # identifying last point before crossing and first point after crossing
  intersections <- trj |>
    dplyr::filter(
      VOL_AS != dplyr::lead(VOL_AS, default = dplyr::last( VOL_AS)) |
        VOL_AS != dplyr::lag( VOL_AS, default = dplyr::first(VOL_AS))
    )

  # establish linestrings for successive trajectory points that mark crossing
  # TODO - if point spacing is 1 sec (or < 3), it is save to pick the closest point as approximation
  xpairs <- duplicate_successive_pairs(intersections)

  # coerce pairs to linestring and determine intersection points with airspace
  # coerce .airspace_poly_sf to multilinestring
  airspace_mls <- .airspace_poly_sf |>
    sf::st_cast( "MULTILINESTRING", group_or_split = FALSE)
  # intersect grouped xpairs with airspace_mls
  # add the PREV_SEQ to account for sequence
  # 1. turn pairs into linestring --------------------------------------------
  xpairs_ls <- xpairs |>
    dplyr::group_by(CROSS) |> #----------- per crossing pair
    dplyr::mutate(PREV_SEQ = dplyr::first(SEQ_ID)) |>
    dplyr::group_by(CROSS, PREV_SEQ) |>
    dplyr::group_modify(     # turn successive points into linestring
      .f = ~ cast_latlon_to_ls(.x)
      ,.keep = TRUE    # keep grouping
    ) |>
    dplyr::ungroup() |>
    sf::st_as_sf()          # ensure we have sf object again with attributes
  # 2. calculate intersection with airspace mls ------------------------------
  xpoints <- xpairs_ls |>
    sf::st_intersection(airspace_mls) |>
    dplyr::distinct(CROSS, PREV_SEQ, geometry)
  # append LAT LON again and add half-step
  xpoints <- xpoints |> cast_pts_to_latlon(.drop_geometry = FALSE) |>
    dplyr::mutate(SEQ_ID = PREV_SEQ + 0.5)

  # --------------- fill crossing portion ------------------------------------
  # for each crossing point pick PREVIOUS and NEXT from xpairs
  # order and interpolate crossings & fill
  xtripples <- xpairs |>
    dplyr::bind_rows(xpoints) |> dplyr::arrange(SEQ_ID, CROSS)
  # 1. inject xtimes
  xtripples <- xtripples |> dplyr::group_by(CROSS) |> interpolate_time_df()
  # 2. fill missing bits
  xtripples <- xtripples |>
    tidyr::fill(UID) |>
    dplyr::mutate(
      VOL_AS =
        ifelse( is.na(VOL_AS),
                paste(dplyr::lag(VOL_AS), dplyr::lead(VOL_AS), sep = "-")
                ,VOL_AS
        )
    )

  # merge with original trajectory
  # TODO ... think about using rows_update or patch or insert
  trj |> dplyr::bind_rows(xtripples) |>
    dplyr::arrange(SEQ_ID)
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
