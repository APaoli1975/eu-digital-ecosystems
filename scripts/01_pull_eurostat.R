# scripts/01_pull_eurostat.R
# Purpose: Pull Eurostat indicators for four case cities (Tallinn, Barcelona, Athens/Attica, Lisbon/AML),
#          harmonise, filter to high-tech relevant series, and write one tidy CSV+XLSX per city.
# Outputs (per city): data_clean/<city>_indicators.csv and .xlsx

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(stringr)
  library(janitor)
  library(lubridate)
  library(eurostat)  # uses local cache by default
  library(writexl)
  library(purrr)
})

# ---------- Folders ----------
dir.create("data_raw",   showWarnings = FALSE)
dir.create("data_clean", showWarnings = FALSE)
dir.create("outputs",    showWarnings = FALSE)

msg  <- function(...) cat("[01] ", sprintf(...), "\n")

# ---------- Targets & matching rules ----------
# We match by NUTS code to be precise. Mixed NUTS levels are unavoidable across tables.
targets <- tibble::tibble(
  city   = c("Tallinn","Barcelona","Athens","Lisbon"),
  # Primary NUTS code to match (regex-friendly, anchored)
  nuts_code_regex = c("^EE001$", "^ES511$", "^EL30$", "^PT17$"),
  # Human-readable region label for documentation
  region_label    = c("Põhja-Eesti (EE001)","Barcelona (ES511)","Attiki (EL30)","Área Metropolitana de Lisboa (PT17)")
)

# ---------- Indicator list (Eurostat codes) ----------
# R&D expenditure (GERD, regional)
tbl_gerd      <- "rd_e_gerdreg"
# Employment in high-tech & knowledge-intensive sectors (regional)
tbl_htec_emp  <- "htec_emp_reg2"
# Patent applications to EPO by technology field (regional)
tbl_patents   <- "pat_ep_rtec"
# Structural business stats by NACE (regional)
tbl_sbs       <- "sbs_na_ind_r2"
# High-growth enterprises (% of active enterprises, regional)
tbl_highgrow  <- "bd_9pm_r2"
# Venture capital investments (% of GDP, national) — country-level (optional)
tbl_vc_nat    <- "tec00040"

tables <- c(tbl_gerd, tbl_htec_emp, tbl_patents, tbl_sbs, tbl_highgrow, tbl_vc_nat)

msg("Pulling Eurostat tables (cached if already downloaded)...")

# ---------- Helpers ----------
to_year <- function(x) {
  # eurostat returns Date, yearmon, numeric, or character.
  if (inherits(x, "Date"))     return(lubridate::year(x))
  if (inherits(x, "yearmon"))  return(as.integer(floor(as.numeric(x))))
  if (is.numeric(x))           return(as.integer(x))
  if (is.character(x)) {
    # keep first 4 digits that look like a year
    y <- suppressWarnings(as.integer(stringr::str_extract(x, "\\d{4}")))
    return(y)
  }
  rep(NA_integer_, length(x))
}

safe_col <- function(df, nm) if (nm %in% names(df)) df[[nm]] else NA

norm_one <- function(df, code){
  # Standardise core columns across heterogeneous tables and **guarantee year/value present**
  df <- df %>% clean_names()
  
  # geo (NUTS) may be 'geo' or 'geo_code'
  geo <- if ("geo" %in% names(df)) df$geo else if ("geo_code" %in% names(df)) df$geo_code else NA
  
  # time column: prefer 'time', fallbacks 'year', 'time_period'
  time_col <- if ("time" %in% names(df)) df$time else if ("year" %in% names(df)) df$year else if ("time_period" %in% names(df)) df$time_period else NA
  year <- to_year(time_col)
  
  # value may live in 'values' or 'value'
  val  <- if ("values" %in% names(df)) df$values else if ("value" %in% names(df)) df$value else NA_real_
  
  out <- tibble::tibble(
    indicator = code,
    geo       = as.character(geo),
    year      = as.integer(year),
    value     = suppressWarnings(as.numeric(val)),
    unit      = if ("unit" %in% names(df)) as.character(df$unit) else NA_character_,
    nace_r2   = if ("nace_r2" %in% names(df)) as.character(df$nace_r2) else NA_character_,
    indic_sb  = if ("indic_sb" %in% names(df)) as.character(df$indic_sb) else NA_character_,
    s_adj     = if ("s_adj" %in% names(df)) as.character(df$s_adj) else NA_character_,
    label     = if ("geo_label" %in% names(df)) as.character(df$geo_label) else NA_character_
  ) %>%
    filter(!is.na(geo), !is.na(year), !is.na(value))
  out
}

get_tbl <- function(tbl_code){
  # Use cached download if possible
  suppressMessages({
    eurostat::get_eurostat(tbl_code, time_format = "num", cache = TRUE)
  })
}

# ---------- Download ----------
raw_list <- list()
for (tb in tables) {
  msg("Table %s", tb)
  raw <- get_tbl(tb)
  # Minimal diagnostics
  years <- suppressWarnings(range(to_year(raw$time %||% raw$year %||% raw$time_period), na.rm = TRUE))
  n_geo <- length(unique((raw$geo %||% raw$geo_code)))
  msg("  - %s: %s rows | years [%s…%s] | %s geos",
      tb, format(nrow(raw), big.mark=","), ifelse(is.finite(years[1]), years[1], "NA"),
      ifelse(is.finite(years[2]), years[2], "NA"), n_geo)
  readr::write_rds(raw, file.path("data_raw", paste0("eurostat_", tb, ".rds")), compress = "gz")
  raw_list[[tb]] <- raw
}

# ---------- Normalise ----------
norm <- purrr::imap_dfr(raw_list, ~norm_one(.x, .y))

# ---------- Domain filters ----------
# 1) Structural business stats: keep only high-tech relevant NACE
ht_nace <- c("J61","J62","J63","C26","M72")  # Telecom, IT services, Info services, Electronics, R&D
norm <- norm %>% mutate(nace_r2 = ifelse(is.na(nace_r2), "", nace_r2))

norm_sbs <- norm %>%
  filter(indicator == tbl_sbs) %>%
  filter(str_detect(nace_r2, paste0("^(", paste(ht_nace, collapse="|"), ")")))

norm_other <- norm %>% filter(indicator != tbl_sbs)
norm <- dplyr::bind_rows(norm_other, norm_sbs)

# 2) Relabel indicators to human-friendly names
norm <- norm %>%
  mutate(indicator = dplyr::case_when(
    indicator == tbl_gerd     ~ "GERD_regional",
    indicator == tbl_htec_emp ~ "HighTech_Employment_regional",
    indicator == tbl_patents  ~ "HighTech_patents_regional",
    indicator == tbl_sbs      ~ "SBS_hightech_regional",
    indicator == tbl_highgrow ~ "HighGrowth_enterprises_share_regional",
    indicator == tbl_vc_nat   ~ "VentureCapital_pct_GDP_national",
    TRUE ~ indicator
  ))

# ---------- Attach NUTS label if missing (best effort) ----------
norm <- norm %>% mutate(region = dplyr::coalesce(label, geo))

# ---------- City extraction by NUTS code ----------
pick_cities <- function(df, target_tbl){
  # For national VC (% GDP), 'geo' is country code (EE, ES, EL, PT)
  if (target_tbl == "VentureCapital_pct_GDP_national") {
    keep_cc <- c("EE","ES","EL","PT")
    vc <- df %>%
      filter(indicator == target_tbl) %>%
      filter(geo %in% keep_cc) %>%
      mutate(city = dplyr::case_when(
        geo == "EE" ~ "Tallinn",
        geo == "ES" ~ "Barcelona",
        geo == "EL" ~ "Athens",
        geo == "PT" ~ "Lisbon",
        TRUE ~ NA_character_
      )) %>%
      mutate(region = geo) %>%
      select(indicator, geo, year, value, unit, region, city)
    return(vc)
  }
  
  # For regional tables, match by NUTS code regex
  reg <- df %>%
    filter(indicator == target_tbl) %>%
    mutate(city = dplyr::case_when(
      str_detect(geo, targets$nuts_code_regex[targets$city=="Tallinn"])   ~ "Tallinn",
      str_detect(geo, targets$nuts_code_regex[targets$city=="Barcelona"]) ~ "Barcelona",
      str_detect(geo, targets$nuts_code_regex[targets$city=="Athens"])    ~ "Athens",
      str_detect(geo, targets$nuts_code_regex[targets$city=="Lisbon"])    ~ "Lisbon",
      TRUE ~ NA_character_
    )) %>%
    filter(!is.na(city)) %>%
    # overwrite region with a cleaner label when available
    mutate(region = dplyr::case_when(
      city=="Tallinn"   ~ targets$region_label[targets$city=="Tallinn"],
      city=="Barcelona" ~ targets$region_label[targets$city=="Barcelona"],
      city=="Athens"    ~ targets$region_label[targets$city=="Athens"],
      city=="Lisbon"    ~ targets$region_label[targets$city=="Lisbon"],
      TRUE ~ region
    )) %>%
    select(indicator, geo, year, value, unit, nace_r2, indic_sb, s_adj, region, city)
  reg
}

by_tbl <- function(code_label){
  pick_cities(norm, code_label)
}

pieces <- dplyr::bind_rows(
  by_tbl("GERD_regional"),
  by_tbl("HighTech_Employment_regional"),
  by_tbl("HighTech_patents_regional"),
  by_tbl("SBS_hightech_regional"),
  by_tbl("HighGrowth_enterprises_share_regional"),
  by_tbl("VentureCapital_pct_GDP_national")
)

# ---------- Final tidy & write per city ----------
final <- pieces %>%
  arrange(city, indicator, year) %>%
  mutate(
    geo_level = case_when(
      str_detect(geo, "^[A-Z]{2}\\d{2,3}$") ~ "NUTS2/3",
      str_detect(geo, "^[A-Z]{2}$")         ~ "Country",
      TRUE ~ "Unknown"
    )
  )

# Year sanity report
yr_by_city <- final %>%
  group_by(city) %>%
  summarise(min_year = suppressWarnings(min(year, na.rm = TRUE)),
            max_year = suppressWarnings(max(year, na.rm = TRUE)),
            .groups = "drop")
msg("Eurostat year coverage by city: %s",
    paste(apply(as.data.frame(yr_by_city), 1, function(r)
      sprintf("%s [%s–%s]", r[["city"]], r[["min_year"]], r[["max_year"]])),
      collapse=" | "))

write_city <- function(cty){
  out <- final %>% filter(city == cty)
  fp_csv <- file.path("data_clean", paste0(tolower(cty), "_indicators.csv")) %>% gsub(" ", "_", .)
  fp_xls <- file.path("data_clean", paste0(tolower(cty), "_indicators.xlsx")) %>% gsub(" ", "_", .)
  readr::write_csv(out, fp_csv)
  writexl::write_xlsx(out, fp_xls)
  msg(" - wrote %s (%s rows)", fp_csv, format(nrow(out), big.mark=","))
  msg(" - wrote %s", fp_xls)
}

msg("Writing per-city outputs to data_clean/ ...")
unique(final$city) %>% purrr::walk(write_city)

# Optional: quick summary to outputs/
sum_tbl <- final %>%
  count(city, indicator, name = "rows") %>%
  arrange(city, indicator)
readr::write_csv(sum_tbl, "outputs/eurostat_summary_rows_by_city_indicator.csv")

msg("✅ Eurostat pull complete.")

