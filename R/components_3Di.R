#' 3Di components derived from milestone table of a trajectory
#'
#'
calc_3Di_comp_flight <- function(.mst_trj, ..., .debug = FALSE){
  if(.debug){
    msg <- paste("Calculating 3Di components for ", unique(.mst_trj$UID))
    message(msg)
  }

  h_comp <- calc_horizontal_comp_flight(.mst_trj, ...)
  v_comp <- calc_vertical_comp_flight(  .mst_trj, ...)

  hv <- dplyr::bind_rows(h_comp, v_comp)
}

calc_3Di_comp_flight_multimst <- function(.multi_mst, ..., .debug = FALSE){
  # for group_modify establish outer-loop var
  this_multi_mst <- .multi_mst |> dplyr::mutate(OUID = UID)

  comps <- this_multi_mst |>
    dplyr::group_by(OUID) |>
    dplyr::group_modify(.f = ~ calc_3Di_comp_flight(.x, .debug = .debug)) |>
    dplyr::ungroup() |> dplyr::select(-OUID)
  return(comps)
}

#' horizontal compoenent
calc_horizontal_comp_flight <- function(.mst_trj, ..., .debug = TRUE){
  h_comp <- .mst_trj |>
    dplyr::filter(MST %in% c("START","STOP")) |>
    dplyr::group_by(UID) |>
    dplyr::summarise(
       PHASE      = "FLIGHT"
      ,SCOPE      = paste0(dplyr::first(MST),"-", dplyr::last(MST))
      ,FLOWN_ACT  = dplyr::last(DIST_FLOWN) - dplyr::first(DIST_FLOWN)
      ,TIME_FLOWN = difftime(dplyr::last(TIME), dplyr::first(TIME), units = "min") |> as.numeric()
      , GCD       = geosphere::distHaversine(
                       cbind(dplyr::first(LON), dplyr::first(LAT))
                      ,cbind(dplyr::last(LON),  dplyr::last(LAT) )
                      , r = 3443.92     # earth radius in Nautical Miles
      )
    ) %>%
    dplyr::mutate(RTE_EXT = (FLOWN_ACT - GCD) / GCD)
}


#' vertical component
calc_vertical_comp_flight <- function(.mst_trj, ..., .debug = TRUE){
  # ------- get toc and tod times
  toc_time <- .mst_trj %>% dplyr::filter(MST == "TOC") %>% dplyr::pull(TIME)
  tod_time <- .mst_trj %>% dplyr::filter(MST == "TOD") %>% dplyr::pull(TIME)
  t_start_stop <- difftime(max(.mst_trj$TIME), min(.mst_trj$TIME), units = "min") %>% as.numeric()

  #--------- phase times
  dur_to_toc  <- difftime(toc_time, min(.mst_trj$TIME), units = "min") |> as.numeric()
  dur_enroute <- difftime(tod_time, toc_time,           units = "min") |> as.numeric()
  dur_fm_tod  <- difftime(max(.mst_trj$TIME), tod_time, units = "min") |> as.numeric()

  max_fl <- .mst_trj |> dplyr::pull(FL) |> max(na.rm = TRUE) |> round()

  # -------- assign flight phase
  # extract levels segments and assign phase
  this_mst <- .mst_trj |>
    dplyr::filter(grepl(pattern = "^LVL", x = MST)) |>
    dplyr::mutate(PHASE = dplyr::case_when(
       TIME < toc_time                      ~ "CLIMB"
      ,toc_time <= TIME & TIME <= tod_time  ~ "ENROUTE"
      ,tod_time < TIME                      ~ "DESCENT"
      ,TRUE ~ NA_character_
      )
    , .after = UID)

  # extract duration and level per level segment
  this_mst <- this_mst |>
    dplyr::mutate(LVL_CNT = ((row_number()-1) %/% 2)) |>
    dplyr::group_by(UID, PHASE, LVL_CNT) |>
    dplyr::summarise(
        T_LVL = difftime(last(TIME), first(TIME), units = "min")
      , FL_LVL = mean(FL) |> round()
      , .groups = "drop")

  # -------- add max fl observed & times
  this_mst <- this_mst |> dplyr::mutate(FL_MAX = max_fl)
  this_mst <- this_mst |> dplyr::mutate(
      T_PHASE = dplyr::case_when(
            PHASE == "CLIMB" ~ dur_to_toc
           ,PHASE == "ENROUTE" ~ dur_enroute
           ,PHASE == "DESCENT" ~ dur_fm_tod)
    , T_TOLD = t_start_stop)
  return(this_mst)
}

