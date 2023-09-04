#' Check for level portions of flight
#'
#' @param .trj
#' @param VV_threshold
#' @param Time_threshold
#'
#' @return
#' @export
#'
#' @examples
check_level_flight <- function(.trj
                               , VV_threshold = 3/60   # FL/min, min = 60 seconds
                               , Time_threshold= 20    # seconds := min length of level
                               , .alt_var=FL
){
  Alt_threshold=VV_threshold*Time_threshold #FL
  ## wrapper for Sam's CDO/CCO level identification
  df <- .trj
  df <- df %>%
    mutate(SEQ_ID = row_number()) %>%
        rename(FLIGHT_LEVEL = {{.alt_var}}, TIME_OVER = TIME) %>%

    mutate(
      LEVEL=ifelse(SEQ_ID,
                   ifelse(abs((FLIGHT_LEVEL-lead(FLIGHT_LEVEL))/as.numeric(difftime(TIME_OVER, lead(TIME_OVER), units="secs")))<=VV_threshold, 1, 0),
                   ifelse(is.na(ifelse(abs(FLIGHT_LEVEL-lead(FLIGHT_LEVEL))<=Alt_threshold, 1, 0)), 0,
                          ifelse(abs(FLIGHT_LEVEL-lead(FLIGHT_LEVEL)) <= Alt_threshold, 1, 0))),
      BEGIN=ifelse(LEVEL==1 & (lag(LEVEL)==0 | TIME_OVER==min(TIME_OVER)), 1, 0),
      END=ifelse(LEVEL==0 & TIME_OVER!=min(TIME_OVER) & lag(LEVEL)==1, 1, 0)
    )
  ## re-wrap
  df <- df %>% rename({{.alt_var}} := FLIGHT_LEVEL, TIME = TIME_OVER)

}



#' Grab and extract level segments
#'
get_level_segments <- function(.trj, ...){
  df <- .trj |>
    check_level_flight() |>

   dplyr::filter(((BEGIN == 1) | (END == 1)) ) |>
   dplyr::mutate(CUM_SUM = cumsum(LEVEL)) |>
    dplyr::group_by(CUM_SUM) |>
    # -- correct for single LVL START or LVL END
    # -- if trj has no level segment (TOC - TOD), return empty
    # ----- for this to work in pipe we mutate() a subsetted vec[for an empty group]
    dplyr::mutate(N = n()) |>
    dplyr::filter(N > 1) |>
    dplyr::mutate(MST = c("LVL_START", "LVL_END")[seq_along(nrow(.trj))]) |>
    dplyr::ungroup() |>
    dplyr::select(-N)   # remove counter and return
}
