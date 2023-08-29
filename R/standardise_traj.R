#' Standardise trajectory state vectors from different sources.
#'
#' Ensure consistent nameing conventions and data types
#'
#' @param .svs_tfclib
#'
#' @return
#' @export
#'
#' @examples
standardise_traj_from_trafficlib <- function(.svs_tfclib){
  svs <- .svs_tfclib |>
    # apply standard names
    dplyr::rename(
       ICAO24 = icao24
      ,FLTID  = callsign
      ,TIME   = timestamp
      ,ALT    = altitude
      ,ALT_G  = geoaltitude
      ,LAT    = latitude
      ,LON    = longitude
    ) |>
   # coerce altitude values & code FL
   # dplyr::mutate(
   #    ALT     = coerce_meter_to_feet(ALT)
   #   ,ALT_G   = coerce_meter_to_feet(ALT_G)
   #   ) |>
    #note ------- traffic provides ALT already in feet!
    dplyr::mutate(FL = (ALT / 100) |> round(digits = 2))
   # reshuffle columns
   svs <- svs |>
     dplyr::select(FLTID, ICAO24, TIME, FL, ALT, LAT, LON, ALT_G, everything())
}
