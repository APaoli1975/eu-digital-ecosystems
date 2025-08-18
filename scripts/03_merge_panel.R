# scripts/03_merge_panel.R
# Purpose: Merge Eurostat city indicators + OECD FUA macros (+ SME finance) into one panel
# Output:  data_clean/analysis_ready_4cities.csv

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(stringr); library(janitor)
})

msg <- function(...) cat("[03-merge] ", sprintf(...), "\n")

# ---- Inputs from 01 (Eurostat) ----
fp_bcn <- "data_clean/barcelona_indicators.csv"
fp_tln <- "data_clean/tallinn_indicators.csv"
fp_ath <- "data_clean/athens_indicators.csv"
fp_lis <- "data_clean/lisbon_indicators.csv"

req_files <- c(fp_bcn, fp_tln, fp_ath, fp_lis)
missing <- req_files[!file.exists(req_files)]
if (length(missing)) stop("Missing Eurostat city files: ", paste(missing, collapse=", "), call. = FALSE)

rd <- function(p) readr::read_csv(p, show_col_types = FALSE) |> janitor::clean_names()

eu <- bind_rows(
  rd(fp_bcn) |> mutate(city = "Barcelona"),
  rd(fp_tln) |> mutate(city = "Tallinn"),
  rd(fp_ath) |> mutate(city = "Athens"),
  rd(fp_lis) |> mutate(city = "Lisbon")
) |>
  mutate(year = suppressWarnings(as.integer(year))) |>
  select(city, indicator, year, value, unit, nace_r2, indic_sb, s_adj, region, geo, geo_level) |>
  arrange(city, indicator, year)

msg("Eurostat rows: %s | indicators: %s",
    format(nrow(eu), big.mark=","), length(unique(eu$indicator)))

# ---- FUA macros from 02 ----
fp_fua <- "data_clean/oecd_region_econom_barcelona_tallinn_athens_lisbon.csv"
if (!file.exists(fp_fua)) stop("Missing FUA file: ", fp_fua, call. = FALSE)

fua_long <- readr::read_csv(fp_fua, show_col_types = FALSE) |>
  janitor::clean_names() |>
  mutate(year = suppressWarnings(as.integer(year)))

# Measures we want as columns
keep_measures <- c("GDP","EMPW","LAB_PROD")
fua_wide <- fua_long |>
  filter(measure %in% keep_measures) |>
  select(city, year, measure, value) |>
  distinct() |>
  tidyr::pivot_wider(names_from = measure, values_from = value)

msg("FUA macro coverage: %s rows | measures: %s",
    format(nrow(fua_wide), big.mark=","), paste(intersect(keep_measures, names(fua_wide)), collapse=", "))

# ---- SME finance (optional) ----
fp_sme <- "data_clean/oecd_sme_fin_ee_es_el_pt.csv"
sme_wide <- NULL
if (file.exists(fp_sme)) {
  sme <- readr::read_csv(fp_sme, show_col_types = FALSE) |>
    janitor::clean_names() |>
    mutate(year = suppressWarnings(as.integer(year)))
  
  if (nrow(sme) > 0) {
    # Map city -> country
    city_country <- tibble::tibble(
      city = c("Tallinn","Barcelona","Athens","Lisbon"),
      country = c("Estonia","Spain","Greece","Portugal")
    )
    sme_wide <- sme |>
      select(country, year, measure, value) |>
      group_by(country, year, measure) |>
      summarise(value = mean(suppressWarnings(as.numeric(value)), na.rm = TRUE), .groups = "drop") |>
      tidyr::pivot_wider(names_from = measure, values_from = value) |>
      right_join(city_country, by = "country") |>
      select(city, country, year, everything())
    msg("SME finance coverage: %s rows (non-empty).", format(nrow(sme_wide), big.mark=","))
  } else {
    msg("SME finance file present but empty — skipping SME join.")
  }
} else {
  msg("SME finance file not found — skipping SME join.")
}

# ---- Merge all into analysis panel ----
# Start from Eurostat long; attach FUA macros (wide) by city+year
eu_base <- eu |>
  left_join(fua_wide, by = c("city","year"))

if (!is.null(sme_wide)) {
  eu_base <- eu_base |>
    left_join(sme_wide, by = c("city","year"))
}

out_path <- "data_clean/analysis_ready_4cities.csv"
readr::write_csv(eu_base, out_path)
msg("✅ Wrote %s (%s rows, %s cols).", out_path, format(nrow(eu_base), big.mark=","), ncol(eu_base))
