#' Number sequences of occurrences
#'
#' kudos to: https://stackoverflow.com/questions/55606323/create-counter-for-runs-of-true-among-false-and-na-by-group
label_runs <- function(.df, .criterium_col, .outside_na = TRUE){
  df <- .df %>%
    dplyr::mutate(RUN = f1( {{ .criterium_col }} ) )

  if(.outside_na){
    # set the labels outside a run to NA, otherwise it will be 0
    df <- df |>
      dplyr::mutate(RUN = replace(RUN, is.na({{ .criterium_col }})|!{{ .criterium_col }}, NA))
  }
}

#' helper function for run labelling
f1 <- function(x) {
  x[is.na(x)] <- FALSE
  rle1 <- rle(x)
  y <- rle1$values
  rle1$values[!y] <- 0
  rle1$values[y] <- cumsum(rle1$values[y])
  return(inverse.rle(rle1))
}
