#' Fix NM trajectory format
#'
read_and_fix_NM_trjs <- function(fn){
  trjs <- readr::read_csv(fn, show_col_types = FALSE) |>
    mutate(LON_QUIRK = LON)

  if(class(trjs$LON) == "character"){
   trjs <- trjs |>
     tidyr::separate(LON, sep = ",", into = c("A","B","C")) |>
     dplyr::mutate(MURKS = is.na(C)) |>
     fix_lon_quirk() |>

     mutate( LAT = paste(LAT, A, sep = ".") |> as.numeric()
            ,LON = paste(B,   C, sep = ".") |> as.numeric())

  }
}

fix_lon_quirk <- function(poss){
  poss <- poss |> dplyr::mutate(C = ifelse(MURKS & grepl(pattern = "^-", x = B), 0, C))
  poss <- poss |> dplyr::mutate(C = ifelse(MURKS & nchar(B) <= 3, 0, C))
  poss <- poss |> dplyr::mutate( C = ifelse(MURKS & nchar(A) <= 3, B, C)
                          ,B = ifelse(MURKS & nchar(A) <= 3, A, B)
                          ,A = ifelse(MURKS & nchar(A) <= 3, 0, A)
  )
}
