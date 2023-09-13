#' Extract 3Di milestones from trajectory
#'
extract_mst_per_trj <- function(.trj, .airspace_sf = NULL ,...){
  # try to fix trj with issues
  if(sum(is.na(.trj$TIME) > 0)){.trj <- .trj |>  dplyr::mutate(TIME = zoo::na.approx(TIME, DIST_FLOWN) |> lubridate::as_datetime())}
  # get first and last airborne position
  start_stop <- .trj |> get_start_stop()

  # get level segments
  lvl_segs <- .trj |> get_level_segments()

  # nail down TOC and TOD based on lvl_segs
  toc_tod <- lvl_segs |>
    dplyr::arrange(FL, TIME) |>
    head(2) |>
    # -------------------------------- catch failing mutate if lvl_segs empty
    dplyr::mutate(MST = c("TOC","TOD")[seq_along(dplyr::row_number())])

  # determines airspace crossings
  xposs <- NULL
  if(!is.null(.airspace_sf)){
    xposs <- .trj |>
      inject_airspace_crossings(.airspace_sf |> select(VOL_AS = id)) |>
      sf::st_drop_geometry() |>
      #----- filter all numbers with decimals
      # ---- inject returns a .5 for the injected crossing - TODO improve
      dplyr::filter(grepl("\\.[[:digit:]]",x = SEQ_ID)) |>
      dplyr::mutate(MST = VOL_AS)
   }

  # glue all together
  trj_mst <- dplyr::bind_rows(start_stop, lvl_segs, toc_tod) |>
    dplyr::select(UID, TIME, FL, LAT, LON, DIST_FLOWN, MST, everything())

  if(!is.null(xposs)){
    trj_mst <- trj_mst |> dplyr::bind_rows(xposs)
   }

  trj_mst <- trj_mst |>  dplyr::arrange(TIME)
  return(trj_mst)
}

#' start stop of trajectory
#'
get_start_stop <- function(.trj, ...){
  .trj |>
    # testing observation: if TIME NA, filter only returns NA stamps
    # todo - enforce tidy input trajectories = all 4D filled
    # workaround: na.rm = TRUE to ignore imperfections
    dplyr::filter(TIME %in% c(min(TIME, na.rm = TRUE), max(TIME, na.rm = TRUE))) |>
    dplyr::mutate(MST = c("START","STOP"))
}
