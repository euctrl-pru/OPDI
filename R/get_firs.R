#' Get FIRs from {pruatlas}
#'
#' Query {pruatlas} for FIR boundaries of EUROCONTROL Member States (and friends).
#'
#' @return
#' @export
#'
#' @examples
get_firs <- function(){
  ms_firs <- pruatlas::country_fir(
     pruatlas::firs_nm_406
    ,icao_id = "E.|L.|UD|UG|GM|UK|GC|BI"
    ,merge = FALSE)
  return(ms_firs)
}

get_borealis_firs <- function(){
  # get all FIRs
  ms_firs <- get_firs()
  # subset for Borealis Alliance --------------
  # Borealis Alliance:
  # Avinor (Norway)
  # ANS Finland (Finland)
  # Irish Aviation Authority (Ireland)
  # Isavia ANS (Iceland)
  # Lennuliiklusteeninduse AS (Estonia)
  # Latvijas Gaisa Satiksme (Latvia)
  # LFV (Sweden)
  # NATS (UK)
  # Naviair (Denmark) -------------------------
  borealis_firs <- ms_firs %>%
    dplyr::filter(
      icao %in% c(
        "EN", # Norway
        "EF", # Finland
        "EI", # Ireland
        "BI", # Iceland
        "EE", # Estonia
        "EV", # Latvia
        "ES", # Sweden
        "EG", # United Kingdom
        "EK"  # Denmark
      )
      )
  return(borealis_firs)
}
