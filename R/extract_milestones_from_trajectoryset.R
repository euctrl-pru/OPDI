trouble_shoot <- function(.trj, .airspace_sf ,...){
  message(paste("Processing ", unique(.trj$UID)))
  this_mst <- .trj |> extract_mst_per_trj(.airspace_sf)
}

extract_mst_from_trjectory_set <- function(.trjs, .airspace_sf = NULL, .debug = FALSE, ...){
  multi_mst <- .trjs |>
    # need to add "outer" UID to make group_modify work (and keep UID in loop)
    dplyr::mutate(OUID = UID) |>
    dplyr::group_by(OUID)

  if(.debug){
    multi_mst <- multi_mst |> dplyr::group_modify(.f = ~ trouble_shoot(.x, .airspace_sf))
  }else{
    multi_mst <- multi_mst |> dplyr::group_modify(.f = ~ extract_mst_per_trj(.x, .airspace_sf, ...))
  }

  multi_mst <- multi_mst |>
    dplyr::ungroup() |>
    dplyr::select(-OUID) # remove OUID
  return(multi_mst)
}
