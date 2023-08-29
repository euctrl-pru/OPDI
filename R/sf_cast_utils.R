#' Utility functions to cast lat/lon data frame/tibble to sf-points and/or linestring
#'
#' @return
NULL

#' @rdname cast_latlon_to_pts
#'
#' @param .df dataframe/tibble of lat/lon positions
#' @param .crs coordinate reference system (default: 4326 := WGS84)
#' @param .drop_coord remove (or keep) lat/lon columns (default: TRUE := remove, FALSE := keep)
#'
#' @return
#' @export
#'
#' @examples
#' \donotrun{
#' cast_latlon_to_pts(adsb_df)
#' }
cast_latlon_to_pts <- function(.df, .crs = 4326, .drop_coord = FALSE){
  pts_sf <- .df %>%
    sf::st_as_sf(coords = c("LON","LAT")
                 , crs = .crs
                 , dim = "XYZ"
                 , agr = "constant"
                 , remove = .drop_coord
                 )
  return(pts_sf)
}

#' @rdname cast_pts_to_ls
#'
#' @return sf linestring
#'
#' @export
cast_pts_to_ls <- function(.pts_sf, .group_var){
  ls_sf <- .pts_sf %>%
    dplyr::group_by({{ .group_var}}) %>%
    dplyr::summarise(do_union = FALSE, .groups = "drop") %>%
    sf::st_cast("LINESTRING")
  return(ls_sf)
}

#' @rdname cast_latlon_to_ls
#'
#' @return sf linestring
#' @export
cast_latlon_to_ls <- function(.df, .crs = 4326, .drop_coord = TRUE, ...){
  pts_sf <- cast_latlon_to_pts(.df, .crs, .drop_coord)
  ls_sf  <- cast_pts_to_ls(pts_sf, .group_var = NULL)
}

#' @rdname cast_point_to_latlon
#'
#' @param .crs
#' @param ...
#' @param .df_pts
#' @param .drop_geometry
#'
#' @return df with LAT/LON coordinates
#' @export
cast_pts_to_latlon <- function(.df_pts, lat_var=LAT, lon_var=LON, .crs = 4326, .drop_geometry = TRUE, ...){
  df <- .df_pts %>%
    dplyr::mutate({{lon_var}} := sf::st_coordinates(.df_pts)[,1],
                  {{lat_var}} := sf::st_coordinates(.df_pts)[,2])

  if(isTRUE(.drop_geometry)){
    df <- df %>% sf::st_drop_geometry()
  }
 return(df)
}
