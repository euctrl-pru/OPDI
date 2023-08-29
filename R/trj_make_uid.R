#' assign an UID for the study by attributing ICAO24 and LEG
#'
#' The difference between successive trajectory points is given. ==> identify legs.
#' It is assumed that the variable LEG is included.
#'
#'
#' @param .trjs_with_legs
#' @param .dof
#'
#' @return tibble of trajectory points appended with UID and commentary for trouble shooting.
#' @export
#'
#' @examples
make_uid <- function(.trjs_with_legs){
  my_trjs <- .trjs_with_legs |>
    dplyr::group_by(ICAO24, LEG) |>
    dplyr::mutate(
       UID = paste( ICAO24
          # assign DOF to min(TIME)
                   , min(lubridate::date(TIME)) |> format(format = "%Y%m%d")
                   , LEG
                   , FLTID      # TODO ... handle missing/weird/multiple FLTIDs
                   , sep = "-")
          # add commentary in case we have several FLTIDs
      ,COM_UID = paste(unique(FLTID), collapse = ", ")
      ) |>
    dplyr::ungroup() |>
    dplyr::select(UID, dplyr::everything())
  return(my_trjs)
}
