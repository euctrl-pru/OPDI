#' Title
#'
#' @param prev_point
#' @param next_point
#' @param crossing_point
#'
#' @return crossing time for intermediate point
#' @export
#'
#' @examples
interpolate_time <- function(prev_point, next_point, crossing_point) {
  # Calculate distances between points using st_distance
  dist_prev_next <- sf::st_distance(
      sf::st_as_sf(prev_point, coords = c("LON", "LAT")),
      sf::st_as_sf(next_point, coords = c("LON", "LAT"))
      )
  dist_prev_crossing <- sf::st_distance(
      sf::st_as_sf(prev_point, coords = c("LON", "LAT")),
      sf::st_as_sf(crossing_point, coords = c("LON", "LAT"))
      )

  # Convert distances from meters to kilometers
  dist_prev_next_km <- as.numeric(dist_prev_next) / 1000
  dist_prev_crossing_km <- as.numeric(dist_prev_crossing) / 1000

  # Convert TIME from difftime to numeric
  time_diff_seconds <- as.numeric(next_point$TIME - prev_point$TIME)

  # Calculate velocities based on the time difference
  velocity_prev_next <- dist_prev_next_km / time_diff_seconds

  # Calculate the time difference at the crossing point
  time_diff_prev_crossing <- dist_prev_crossing_km / velocity_prev_next

  # Calculate the crossing time
  crossing_time <- prev_point$TIME + as.difftime(time_diff_prev_crossing, units = "secs")

  # Convert ALT from difftime to numeric
  alt_diff <- as.numeric(next_point$FL - prev_point$FL)

  # Calculate linearly interpolated altitude at the crossing point
  crossing_altitude <- prev_point$FL + (as.numeric(crossing_time - prev_point$TIME)) *
    (alt_diff / (as.numeric(next_point$TIME - prev_point$TIME)))

  return(list(crossing_time, crossing_altitude))
}

#'
#'
interpolate_time_df <- function(.crossing_tripplet){
  prev_point     <- .crossing_tripplet |> dplyr::filter(TIME == min(TIME, na.rm = TRUE))
  crossing_point <- .crossing_tripplet |> dplyr::filter(is.na(TIME))
  next_point     <- .crossing_tripplet |> dplyr::filter(TIME == max(TIME, na.rm = TRUE))

  crossing_point$TIME <- interpolate_time(prev_point, next_point, crossing_point)[[1]]
  crossing_point$FL   <- interpolate_time(prev_point, next_point, crossing_point)[[2]]

  return(dplyr::bind_rows(prev_point, crossing_point, next_point))
}

