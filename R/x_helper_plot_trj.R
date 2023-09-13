#' quick ploy helper to visualise trajectories
#'
plot_horizontal <- function(.pick_trj){
  hp <- .pick_trj |>
    ggplot() +
    geom_point(aes(x = LON, y = LAT))
  return(hp)
}

plot_vertical <- function(.pick_trj){
  vp <- .pick_trj |>
    ggplot() +
    geom_point(aes(x = TIME, y = FL))
  return(vp)
}

plot_hv <- function(.pick_trj){
  hp <- plot_horizontal(.pick_trj)
  vp <- plot_vertical(.pick_trj)
  patchwork::wrap_plots(hp, vp, ncol = 2)
}
