standardise_traj_from_NM <- function(.cpf, ..., .debug = FALSE){
  this_trjs <- .cpf |>
    dplyr::rename(TIME = TIME_OVER, FL = FLIGHT_LEVEL) |>
    dplyr::mutate(TIME = lubridate::dmy_hms(TIME)
                  ,DIST_FLOWN = POINT_DIST
                  ,DIST_FLOWN = DIST_FLOWN / 1.852  # convert km to NM
                  ,SOURCE = "NM"
                  ,UID = SAM_ID
    )
  if(.debug){
    this_trjs <- this_trjs |>
      dplyr::select(UID, TIME, LAT, LON, FL, DIST_FLOWN, SOURCE
                    , dplyr::everything())
  }else{
    this_trjs <- this_trjs |>
      dplyr::select(UID, TIME, LAT, LON, FL, DIST_FLOWN, SEQ_ID, SOURCE)
  }
}
