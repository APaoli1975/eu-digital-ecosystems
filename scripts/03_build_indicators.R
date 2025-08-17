# scripts/03_build_indicators.R
suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(tidyr); library(janitor)
})

# ---------- helpers ----------
msg <- function(...) cat("[03] ", sprintf(...), "\n")
rd  <- function(p) readr::read_csv(p, show_col_types = FALSE) |> janitor::clean_names()

robust_year <- function(x){
  if (inherits(x, "Date"))    return(as.integer(format(x, "%Y")))
  if (inherits(x, "yearmon")) return(as.integer(floor(as.numeric(x))))
  if (is.numeric(x))          return(as.integer(x))
  if (is.character(x)) {
    # try "YYYY" anywhere in the string
    y <- suppressWarnings(as.integer(stringr::str_extract(x, "(?<!\\d)\\d{4}(?!\\d)")))
    return(y)
  }
  suppressWarnings(as.integer(x))
}

# Return first matching column (case-insensitive, regex allowed); else NULL
pick_any_col <- function(df, patterns){
  nms <- names(df)
  low <- tolower(nms)
  for (p in patterns) {
    hit <- which(str_detect(low, regex(p, ignore_case = TRUE)))
    if (length(hit)) return(nms[hit[1]])
  }
  NULL
}

# ---------- inputs ----------
eu_raw_dir <- "data_raw"
tbl_gerd      <- "rd_e_gerdreg"
tbl_htec_emp  <- "htec_emp_reg2"
tbl_patents   <- "pat_ep_rtec"
tbl_sbs       <- "sbs_na_ind_r2"
tbl_highgrow  <- "bd_9pm_r2"

fp_oecdF <- "data_clean/oecd_region_econom_barcelona_tallinn_athens_lisbon.csv"
fp_oecdS <- "data_clean/oecd_sme_fin_ee_es_el_pt.csv"
if (!file.exists(fp_oecdF)) stop("Missing file: ", fp_oecdF, call. = FALSE)
if (!file.exists(fp_oecdS)) stop("Missing file: ", fp_oecdS, call. = FALSE)

# ---------- REBUILD Eurostat from raw ----------
rfp <- function(code){
  csv <- file.path(eu_raw_dir, paste0("eurostat_", code, ".csv"))
  rds <- file.path(eu_raw_dir, paste0("eurostat_", code, ".rds"))
  if (file.exists(csv)) return(csv)
  if (file.exists(rds)) return(rds)
  NA_character_
}

read_eu_raw <- function(code){
  path <- rfp(code)
  if (is.na(path)) { msg("⚠ Missing raw for %s", code); return(NULL) }
  out <- if (endsWith(path, ".csv")) rd(path) else readr::read_rds(path)
  janitor::clean_names(out)
}

norm_eu <- function(df, label){
  if (is.null(df)) return(NULL)
  # try many variants used by SDMX/Eurostat dumps
  geo_col  <- pick_any_col(df, c("^geo$", "^geo_code$", "^ref.?_?area$", "^region$", "^nuts"))
  time_col <- pick_any_col(df, c("^year$", "^time$", "^time_?period$", "^obs_?time$", "^obstime$", "^timeperiod$"))
  val_col  <- pick_any_col(df, c("^values?$", "^obs_?value$", "^obsvalue$", "^value$"))
  
  if (is.null(geo_col))  stop("Eurostat: cannot find GEO column in: ", paste(names(df), collapse=", "))
  if (is.null(time_col)) msg("ℹ Eurostat: no explicit time column; will try to derive year from other cols.")
  if (is.null(val_col))  stop("Eurostat: cannot find VALUE column in: ", paste(names(df), collapse=", "))
  
  geo <- df[[geo_col]]
  tim <- if (!is.null(time_col)) df[[time_col]] else NA
  val <- df[[val_col]]
  
  yr  <- robust_year(tim)
  
  tibble::tibble(
    indicator = label,
    geo       = as.character(geo),
    year      = yr,
    value     = suppressWarnings(as.numeric(val))
  ) |> filter(!is.na(geo))
}

eu_all <- dplyr::bind_rows(
  norm_eu(read_eu_raw(tbl_gerd),     "GERD_regional"),
  norm_eu(read_eu_raw(tbl_htec_emp), "HighTech_Employment_regional"),
  norm_eu(read_eu_raw(tbl_patents),  "HighTech_patents_regional"),
  norm_eu(read_eu_raw(tbl_sbs),      "SBS_hightech_regional"),
  norm_eu(read_eu_raw(tbl_highgrow), "HighGrowth_enterprises_share_regional")
)

if (is.null(eu_all) || nrow(eu_all) == 0) {
  stop("Eurostat raw tables not found / empty in data_raw/. Run scripts/01_pull_eurostat.R first.")
}

# Flexible NUTS matching (NUTS3 or NUTS2 parent)
targets <- tibble::tibble(
  city         = c("Tallinn","Barcelona","Athens","Lisbon"),
  nuts3_exact  = c("EE001","ES511","EL305|EL30","PT170|PT17"),
  nuts2_parent = c("EE00","ES51","EL30","PT17")
) |>
  mutate(nuts_pattern = paste0("^(", nuts3_exact, ")$|^(", nuts2_parent, ")"))

pat_for <- function(cty) targets$nuts_pattern[targets$city == cty]

eu_city <- eu_all |>
  mutate(city = dplyr::case_when(
    str_detect(geo, pat_for("Tallinn"))   ~ "Tallinn",
    str_detect(geo, pat_for("Barcelona")) ~ "Barcelona",
    str_detect(geo, pat_for("Athens"))    ~ "Athens",
    str_detect(geo, pat_for("Lisbon"))    ~ "Lisbon",
    TRUE ~ NA_character_
  )) |>
  filter(!is.na(city))

# Some tables come as national (2-letter geo) — map to cities so they still appear
eu_nat <- eu_all |>
  filter(geo %in% c("EE","ES","EL","PT")) |>
  mutate(city = case_when(
    geo == "EE" ~ "Tallinn",
    geo == "ES" ~ "Barcelona",
    geo == "EL" ~ "Athens",
    geo == "PT" ~ "Lisbon",
    TRUE ~ NA_character_
  )) |>
  filter(!is.na(city))

eu_ind <- bind_rows(eu_city, eu_nat) |>
  group_by(city, indicator, year) |>
  summarise(value = sum(value, na.rm = TRUE), .groups = "drop")

msg("Eurostat rebuild → %s rows | years [%s…%s]",
    format(nrow(eu_ind), big.mark=","), suppressWarnings(min(eu_ind$year, na.rm=TRUE)),
    suppressWarnings(max(eu_ind$year, na.rm=TRUE)))

# ---------- OECD FUA (keep ALL measures; safe names) ----------
fua <- rd(fp_oecdF)
msg("FUA cols: %s", paste(names(fua), collapse = ", "))

pick_col <- function(df, pats, label){
  nm <- names(df)
  for (p in pats) {
    hit <- nm[str_detect(nm, regex(p, ignore_case = TRUE))]
    if (length(hit)) return(hit[1])
  }
  stop("Cannot find ", label, " in: ", paste(nm, collapse=", "))
}

pat_year  <- c("^year$", "time", "time_period", "obstime", "obs_time")
pat_meas  <- c("^measure$", "indicator", "item", "variable")
pat_value <- c("^value$", "obs_value", "val", "amount", "^obsval(ue)?$")

fua_year <- pick_col(fua, pat_year,  "FUA YEAR")
fua_meas <- pick_col(fua, pat_meas,  "FUA MEASURE")
fua_val  <- pick_col(fua, pat_value, "FUA VALUE")

lab <- tolower(if ("region_label" %in% names(fua)) fua$region_label else "")
if ("region_label" %in% names(fua) && requireNamespace("stringi", quietly = TRUE))
  lab <- stringi::stri_trans_general(lab, "Latin-ASCII")

fua <- fua |>
  mutate(city = dplyr::case_when(
    str_detect(lab, "barcel")                                      ~ "Barcelona",
    str_detect(lab, "tallinn")                                     ~ "Tallinn",
    str_detect(lab, "athens|attica|attiki|athin")                  ~ "Athens",
    str_detect(lab, "lisbon|lisboa")                               ~ "Lisbon",
    str_detect(region_code, regex("^ES", ignore_case = TRUE))      ~ "Barcelona",
    str_detect(region_code, regex("^EE", ignore_case = TRUE))      ~ "Tallinn",
    str_detect(region_code, regex("^EL|^GR", ignore_case = TRUE))  ~ "Athens",
    str_detect(region_code, regex("^PT", ignore_case = TRUE))      ~ "Lisbon",
    TRUE ~ NA_character_
  )) |>
  filter(!is.na(city))

msg("FUA measures (first 20): %s", paste(head(sort(unique(fua[[fua_meas]])), 20), collapse = ", "))

fua_wide <- fua |>
  rename(year_raw = all_of(fua_year),
         measure   = all_of(fua_meas),
         value_raw = all_of(fua_val)) |>
  mutate(year  = robust_year(year_raw),
         value = suppressWarnings(as.numeric(value_raw))) |>
  group_by(city, year, measure) |>
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") |>
  mutate(measure_clean = make.names(toupper(trimws(measure)))) |>
  select(city, year, measure_clean, value) |>
  distinct() |>
  pivot_wider(names_from = measure_clean, values_from = value)

msg("FUA wide: %s rows | cols: %s", nrow(fua_wide), paste(names(fua_wide), collapse=", "))

# ---------- OECD SME (country → city; keep ALL measures) ----------
sme <- rd(fp_oecdS)
msg("SME cols: %s", paste(names(sme), collapse = ", "))

sme_year <- pick_col(sme, pat_year,  "SME YEAR")
sme_meas <- pick_col(sme, pat_meas,  "SME MEASURE")
sme_val  <- pick_col(sme, pat_value, "SME VALUE")
sme_ctry <- pick_col(sme, c("^country$","country_name","ref_area","geo","nation","country_iso2","country_iso3"), "SME COUNTRY")

sme_wide <- sme |>
  rename(year_raw = all_of(sme_year),
         country_raw = all_of(sme_ctry),
         indicator    = all_of(sme_meas),
         value_raw    = all_of(sme_val)) |>
  mutate(
    year  = robust_year(year_raw),
    value = suppressWarnings(as.numeric(value_raw)),
    country_up = toupper(as.character(country_raw)),
    country = case_when(
      country_up %in% c("EE","EST","ESTONIA")     ~ "Estonia",
      country_up %in% c("ES","ESP","SPAIN")       ~ "Spain",
      country_up %in% c("EL","GR","GRC","GREECE") ~ "Greece",
      country_up %in% c("PT","PRT","PORTUGAL")    ~ "Portugal",
      TRUE ~ as.character(country_raw)
    )
  ) |>
  group_by(country, year, indicator) |>
  summarise(value = mean(value, na.rm = TRUE), .groups = "drop") |>
  mutate(indicator_clean = make.names(toupper(trimws(indicator)))) |>
  select(country, year, indicator_clean, value) |>
  pivot_wider(names_from = indicator_clean, values_from = value)

msg("SME wide: %s rows | countries: %s",
    nrow(sme_wide), paste(sort(unique(sme_wide$country)), collapse=", "))

# ---------- Final join ----------
city_country <- tibble::tibble(
  city    = c("Tallinn","Barcelona","Athens","Lisbon"),
  country = c("Estonia","Spain","Greece","Portugal")
)

analytical <- eu_ind |>
  left_join(fua_wide,     by = c("city","year")) |>
  left_join(city_country, by = "city") |>
  left_join(sme_wide,     by = c("country","year")) |>
  arrange(city, indicator, year)

out_path <- "data_clean/analysis_ready_4cities.csv"
readr::write_csv(analytical, out_path)
msg("Wrote %s (%d rows, %d cols) — years [%s…%s]",
    out_path, nrow(analytical), ncol(analytical),
    suppressWarnings(min(analytical$year, na.rm = TRUE)),
    suppressWarnings(max(analytical$year, na.rm = TRUE)))

