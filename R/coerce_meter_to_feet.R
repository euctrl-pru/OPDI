#' OSN mtrs to feet
#'
#' Converts a vector of altitudes/heights given in meeter to feet.
#' 1m = 100cm/(2.54cm/in)/(12in/ft) = 3.280839895ft
#'
#' @param .altitude_m altitude measured in metres
#'
#' @return .altitude_ft altitude in ft
#' @export
#'
#' @examples
coerce_meter_to_feet <- function(.altitude_m){
  altitude_ft <- .altitude_m * 3.280839895
}
