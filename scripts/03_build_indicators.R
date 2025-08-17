# scripts/03_build_indicators.R
suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(tidyr); library(janitor)
})

# ----- helpers -----
must <- function(path) if (!file.exists(path)) stop("Missing file: ", path, call. = FALSE)
msg  <- function(...) cat("[03] ", sprintf(...), "\n")
rd   <- function(p) readr::read_csv(p, show_col_types = FALSE) |> janitor::clean_names()

robust_year <- function(x){
  if (inherits(x, "Date"))    return(as.integer(format(x, "%Y")))
  if (inherits(x, "yearmon")) return(as.integer(floor(as.numeric(x))))
  if (is.numeric(x))          return(as.integer(x))
  if (is.character(x))        return(suppressWarnings(as.integer(substr(x, 1L, 4L))))
  suppressWarnings(as.integer(x))
}

# ----- input paths (aligned to 01_ and 02_) -----
fp_bcn   <- "data_clean/barcelona_indicators.csv"
fp_tln   <- "data_clean/tallinn_indicators.csv"
fp_ath   <- "data_clean/athens_indicators.csv"
fp_lis   <- "data_clean/lisbon_indicators.csv"

fp_oecdF <- "data_clean/oecd_region_econom_barcelona_tallinn_athens_lisbon.csv"
fp_oecdS <- "data_clean/oecd_sme_fin_ee_es_el_pt.csv"

invisible(lapply(c(fp_bcn, fp_tln, fp_ath, fp_lis, fp_oecdF, fp_oecdS), must))

# ----- read & tag Eurostat per-city (clean outputs from 01_) -----
bcn <- rd(fp_bcn) |> mutate(city = "Barcelona")
tln <- rd(fp_tln) |> mutate(city = "Tallinn")
ath <- rd(fp_ath) |> mutate(city = "Athens")
lis <- rd(fp_lis) |> mutate(city = "Lisbon")

# harmonise (but do NOT drop NA years)
pat_year      <- c("^year$", "time", "time_period", "obstime", "obs_time")
pat_indicator <- c("^indicator$", "measure", "variable", "item", "ind_code")
pat_value     <- c("^value$", "obs_value", "val", "amount", "^obsval(ue)?$")

pick_col <- function(df, pats, label) {
  nm <- names(df); hit <- NULL
  for (p in pats) {
    m <- nm[stringr::str_detect(nm, stringr::regex(p, ignore_case = TRUE))]
    if (length(m)) { hit <- m[1]; break }
  }
  if (is.null(hit)) stop("Cannot find ", label, " in columns: ", paste(nm, collapse = ", "), call. = FALSE)
  hit
}

harmonise_ind <- function(df) {
  yr  <- pick_col(df, pat_year,      "YEAR")
  ind <- pick_col(df, pat_indicator, "INDICATOR")
  val <- pick_col(df, pat_value,     "VALUE")
  df |>
    rename(year_raw = all_of(yr),
           indicator = all_of(ind),
           value_raw = all_of(val)) |>
    mutate(
      year  = robust_year(year_raw),
      value = suppressWarnings(as.numeric(value_raw)),
      indicator = as.character(indicator)
    ) |>
    select(city, year, indicator, value)
}

eu_ind <- bind_rows(
  harmonise_ind(bcn),
  harmonise_ind(tln),
  harmonise_ind(ath),
  harmonise_ind(lis)
)

# ----- If Eurostat years are NA, rebuild from data_raw eurostat tables -----
all_years_na <- function(x) all(is.na(x)) || !any(!is.na(x))
needs_rebuild <- eu_ind |>
  group_by(city, indicator) |>
  summarise(all_na = all_years_na(year), .groups = "drop") |>
  summarise(any(all_na)) |> pull()

if (isTRUE(needs_rebuild)) {
  msg("Eurostat years mostly NA → rebuilding from data_raw eurostat_* tables...")
  # table codes (as used in 01_)
  tbl_gerd     <- "rd_e_gerdreg"
  tbl_htec_emp <- "htec_emp_reg2"
  tbl_patents  <- "pat_ep_rtec"
  tbl_sbs      <- "sbs_na_ind_r2"
  tbl_highgrow <- "bd_9pm_r2"
  
  # NUTS patterns (flexible: NUTS3 or NUTS2 parent)
  targets <- tibble::tibble(
    city   = c("Tallinn","Barcelona","Athens","Lisbon"),
    nuts3_exact  = c("EE001","ES511","EL305|EL30","PT170|PT17"),
    nuts2_parent = c("EE00","ES51","EL30","PT17"),
    nuts_pattern = paste0("^(", nuts3_exact, ")$|^(", nuts2_parent, ")")
  )
  
  # raw file helper
  rfp <- function(code) {
    # prefer CSV dumped by 01_; fallback to RDS if needed
    csv <- file.path("data_raw", paste0("eurostat_", code, ".csv"))
    rds <- file.path("data_raw", paste0("eurostat_", code, ".rds"))
    if (file.exists(csv)) return(csv)
    if (file.exists(rds)) return(rds)
    NA_character_
  }
  
  read_eu_raw <- function(code){
    path <- rfp(code)
    if (is.na(path)) return(NULL)
    if (endsWith(path, ".csv")) rd(path) else suppressWarnings(readr::read_rds(path))
  }
  
  norm_raw <- function(df, indicator_name){
    if (is.null(df)) return(NULL)
    df <- janitor::clean_names(df)
    geo <- if ("geo" %in% names(df)) df$geo else if ("geo_code" %in% names(df)) df$geo_code else NA
    time_col <- if ("time" %in% names(df)) df$time else if ("year" %in% names(df)) df$year else NA
    val <- if ("values" %in% names(df)) df$values else if ("value" %in% names(df)) df$value else NA
    tibble::tibble(
      indicator = indicator_name,
      geo   = as.character(geo),
      year  = robust_year(time_col),
      value = suppressWarnings(as.numeric(val))
    )
  }
  
  # read + normalise each table
  gerd     <- norm_raw(read_eu_raw(tbl_gerd),     "GERD_regional")
  htec     <- norm_raw(read_eu_raw(tbl_htec_emp), "HighTech_Employment_regional")
  patents  <- norm_raw(read_eu_raw(tbl_patents),  "HighTech_patents_regional")
  sbs      <- norm_raw(read_eu_raw(tbl_sbs),      "SBS_hightech_regional")
  highgrow <- norm_raw(read_eu_raw(tbl_highgrow), "HighGrowth_enterprises_share_regional")
  
  eu_all <- bind_rows(gerd, htec, patents, sbs, highgrow) %>% filter(!is.na(geo))
  
  # city matching by NUTS pattern
  pat_for <- function(cty) targets$nuts_pattern[targets$city == cty]
  eu_city <- eu_all %>%
    mutate(city = dplyr::case_when(
      str_detect(geo, pat_for("Tallinn"))   ~ "Tallinn",
      str_detect(geo, pat_for("Barcelona")) ~ "Barcelona",
      str_detect(geo, pat_for("Athens"))    ~ "Athens",
      str_detect(geo, pat_for("Lisbon"))    ~ "Lisbon",
      TRUE ~ NA_character_
    )) %>%
    filter(!is.na(city))
  
  # In case some tables are national (geo == country code), map them to cities
  national_codes <- c("EE","ES","EL","PT")
  eu_nat <- eu_all %>%
    filter(geo %in% national_codes) %>%
    mutate(city = case_when(
      geo == "EE" ~ "Tallinn",
      geo == "ES" ~ "Barcelona",
      geo == "EL" ~ "Athens",
      geo == "PT" ~ "Lisbon",
      TRUE ~ NA_character_
    )) %>%
    filter(!is.na(city))
  
  eu_ind <- bind_rows(eu_city, eu_nat) %>%
    group_by(city, indicator, year) %>%
    summarise(value = sum(value, na.rm = TRUE), .groups = "drop")
  
  msg("Rebuilt Eurostat — rows: %d, years [%s…%s]", nrow(eu_ind),
      suppressWarnings(min(eu_ind$year, na.rm = TRUE)),
      suppressWarnings(max(eu_ind$year, na.rm = TRUE)))
}

# ----- OECD FUA & SME (unchanged; keep all measures) -----
fua <- rd(fp_oecdF); sme <- rd(fp_oecdS)
msg("FUA cols: %s", paste(names(fua), collapse = ", "))
msg("SME cols: %s", paste(names(sme), collapse = ", "))

# FUA: derive city; keep ALL measures, safe names
fua_year <- pick_col(fua, pat_year, "FUA YEAR")
fua_meas <- pick_col(fua, c("^measure$", "indicator", "item", "variable"), "FUA MEASURE")
fua_val  <- pick_col(fua, pat_value, "FUA VALUE")

fua_has_label <- "region_label" %in% names(fua)
lab <- tolower(if (fua_has_label) fua$region_label else "")
if (fua_has_label && requireNamespace("stringi", quietly = TRUE)) {
  lab <- stringi::stri_trans_general(lab, "Latin-ASCII")
}

fua <- fua %>%
  mutate(city = dplyr::case_when(
    fua_has_label & str_detect(lab, "barcel")                     ~ "Barcelona",
    fua_has_label & str_detect(lab, "tallinn")                    ~ "Tallinn",
    fua_has_label & str_detect(lab, "athens|attica|attiki|athin") ~ "Athens",
    fua_has_label & str_detect(lab, "lisbon|lisboa")              ~ "Lisbon",
    str_detect(region_code, regex("^ES", ignore_case = TRUE))     ~ "Barcelona",
    str_detect(region_code, regex("^EE", ignore_case = TRUE))     ~ "Tallinn",
    str_detect(region_code, regex("^EL|^GR", ignore_case = TRUE)) ~ "Athens",
    str_detect(region_code, regex("^PT", ignore_case = TRUE))     ~ "Lisbon",
    TRUE ~ NA_character_
  )) %>%
  filter(!is.na(city))

msg("FUA unique measures (first 20): %s",
    paste(head(sort(unique(fua[[fua_meas]])), 20), collapse = ", "))

fua_wide <- fua %>%
  rename(year_raw = all_of(fua_year),
         measure   = all_of(fua_meas),
         value_raw = all_of(fua_val)) %>%
  mutate(year = robust_year(year_raw),
         value = suppressWarnings(as.numeric(value_raw))) %>%
  group_by(city, year, measure) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
  mutate(measure_clean = make.names(toupper(trimws(measure)))) %>%
  select(city, year, measure_clean, value) %>%
  distinct() %>%
  tidyr::pivot_wider(names_from = measure_clean, values_from = value)

msg("FUA macro rows (wide): %d; cols: %s", nrow(fua_wide), paste(names(fua_wide), collapse = ", "))

# SME: country-level → city via mapping; keep ALL measures, safe names
sme_year <- pick_col(sme, pat_year, "SME YEAR")
sme_ctry <- pick_col(sme, c("^country$","country_name","ref_area","geo","nation","country_iso2","country_iso3"), "SME COUNTRY")
sme_meas <- pick_col(sme, c("^indicator$","measure","item","variable"), "SME INDICATOR")
sme_val  <- pick_col(sme, pat_value, "SME VALUE")

sme_wide <- sme %>%
  rename(year_raw = all_of(sme_year),
         country_raw = all_of(sme_ctry),
         indicator    = all_of(sme_meas),
         value_raw    = all_of(sme_val)) %>%
  mutate(
    year  = robust_year(year_raw),
    value = suppressWarnings(as.numeric(value_raw)),
    country_up = toupper(as.character(country_raw)),
    country = dplyr::case_when(
      country_up %in% c("EE","EST","ESTONIA")     ~ "Estonia",
      country_up %in% c("ES","ESP","SPAIN")       ~ "Spain",
      country_up %in% c("EL","GR","GRC","GREECE") ~ "Greece",
      country_up %in% c("PT","PRT","PORTUGAL")    ~ "Portugal",
      TRUE ~ as.character(country_raw)
    )
  ) %>%
  group_by(country, year, indicator) %>%
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") %>%
  mutate(indicator_clean = make.names(toupper(trimws(indicator)))) %>%
  select(country, year, indicator_clean, value) %>%
  tidyr::pivot_wider(names_from = indicator_clean, values_from = value)

msg("SME rows (wide): %d; example countries: %s",
    nrow(sme_wide),
    paste(head(sort(unique(sme_wide$country)), 6), collapse = ", "))

# ----- final join for all 4 cities -----
city_country <- tibble::tibble(
  city    = c("Tallinn","Barcelona","Athens","Lisbon"),
  country = c("Estonia","Spain","Greece","Portugal")
)

analytical <- eu_ind %>%
  left_join(fua_wide,     by = c("city","year")) %>%
  left_join(city_country, by = "city") %>%
  left_join(sme_wide,     by = c("country","year"))

out_path <- "data_clean/analysis_ready_4cities.csv"
readr::write_csv(analytical, out_path)
msg("Wrote %s (%d rows, %d cols)", out_path, nrow(analytical), ncol(analytical))

