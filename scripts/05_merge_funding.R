# scripts/05_merge_funding.R
# Purpose: Merge EU funding panel into the analysis panel and add useful ratios & provenance.
# Inputs:
#   - data_clean/analysis_ready_4cities.csv           (from 03_merge_panel.R)
#   - data_clean/eu_funding_panel.csv                 (from 04_fetch_funding.R)
# Output (new master panel):
#   - data_clean/analysis_ready_4cities_with_funding.csv
# Also writes small diagnostics in outputs/

suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(janitor)
  library(stringr)
  library(tidyr)
})

msg <- function(...) cat("[05-funding-merge] ", sprintf(...), "\n")

# ---------- Inputs ----------
fp_panel <- "data_clean/analysis_ready_4cities.csv"
fp_fund  <- "data_clean/eu_funding_panel.csv"

if (!file.exists(fp_panel)) stop("[05-funding-merge] ❌ Missing: ", fp_panel, call. = FALSE)
if (!file.exists(fp_fund))  stop("[05-funding-merge] ❌ Missing: ", fp_fund,  call. = FALSE)

panel_raw <- readr::read_csv(fp_panel, show_col_types = FALSE) |> clean_names()
fund_raw  <- readr::read_csv(fp_fund,  show_col_types = FALSE) |> clean_names()

# ---------- Normalise types & expected columns ----------
panel <- panel_raw %>%
  mutate(
    city = as.character(city),
    year = suppressWarnings(as.integer(year))
  )

# If FUA macro columns exist, ensure numeric; otherwise create as NA (so mutate() below works)
macro_cols <- c("gdp", "empw", "lab_prod")
for (mc in macro_cols) {
  if (!mc %in% names(panel)) panel[[mc]] <- NA_real_
  panel[[mc]] <- suppressWarnings(as.numeric(panel[[mc]]))
}

fund <- fund_raw %>%
  # Expected: city, year, eu_funding, funds_pc (optional), funding_geo, funding_method
  mutate(
    city          = as.character(city),
    year          = suppressWarnings(as.integer(year)),
    eu_funding    = suppressWarnings(as.numeric(eu_funding)),
    funds_pc      = suppressWarnings(as.numeric(if ("funds_pc" %in% names(.)) funds_pc else NA)),
    funding_geo   = if ("funding_geo"   %in% names(.)) as.character(funding_geo)   else NA_character_,
    funding_method= if ("funding_method"%in% names(.)) as.character(funding_method) else NA_character_
  ) %>%
  # keep a single row per city-year (sum across duplicates just in case)
  group_by(city, year) %>%
  summarise(
    eu_funding     = sum(eu_funding, na.rm = TRUE),
    funds_pc       = if (all(is.na(funds_pc))) NA_real_ else mean(funds_pc, na.rm = TRUE),
    funding_geo    = dplyr::first(na.omit(funding_geo)),
    funding_method = dplyr::first(na.omit(funding_method)),
    .groups = "drop"
  )

# ---------- Merge ----------
msg("Panel rows: %s | Funding rows: %s", nrow(panel), nrow(fund))

merged <- panel %>%
  left_join(fund, by = c("city","year")) %>%
  mutate(
    funding_to_gdp  = ifelse(!is.na(eu_funding) & !is.na(gdp)  & gdp  != 0, eu_funding / gdp,  NA_real_),
    funding_per_emp = ifelse(!is.na(eu_funding) & !is.na(empw) & empw != 0, eu_funding / empw, NA_real_)
  )

# ---------- Diagnostics ----------
nn <- function(x) sum(!is.na(x))
msg("Non-NA counts → eu_funding: %d | funds_pc: %d | funding_to_gdp: %d | funding_per_emp: %d",
    nn(merged$eu_funding), nn(merged$funds_pc), nn(merged$funding_to_gdp), nn(merged$funding_per_emp))

# City-year coverage table
diag_cov <- merged %>%
  distinct(city, year, eu_funding, funding_geo, funding_method) %>%
  group_by(city) %>%
  summarise(
    years_with_funding = n_distinct(year[!is.na(eu_funding)]),
    min_year = suppressWarnings(min(year[!is.na(eu_funding)])),
    max_year = suppressWarnings(max(year[!is.na(eu_funding)])),
    funding_geo    = first(na.omit(funding_geo)),
    funding_method = first(na.omit(funding_method)),
    .groups = "drop"
  ) %>%
  arrange(city)

# Intensity snapshot by city (latest year available for each city)
diag_latest <- merged %>%
  filter(!is.na(eu_funding)) %>%
  group_by(city) %>%
  filter(year == max(year, na.rm = TRUE)) %>%
  summarise(
    year               = max(year, na.rm = TRUE),
    eu_funding         = sum(eu_funding, na.rm = TRUE),
    funding_to_gdp     = mean(funding_to_gdp, na.rm = TRUE),
    funding_per_emp    = mean(funding_per_emp, na.rm = TRUE),
    funds_pc           = mean(funds_pc, na.rm = TRUE),
    funding_geo        = first(na.omit(funding_geo)),
    funding_method     = first(na.omit(funding_method)),
    .groups = "drop"
  ) %>% arrange(city)

dir.create("outputs", showWarnings = FALSE)
readr::write_csv(diag_cov,    "outputs/funding_merge_coverage_by_city.csv")
readr::write_csv(diag_latest, "outputs/funding_intensity_latest_by_city.csv")
msg("Wrote outputs/funding_merge_coverage_by_city.csv and outputs/funding_intensity_latest_by_city.csv")

# ---------- Write master panel ----------
out <- "data_clean/analysis_ready_4cities_with_funding.csv"
readr::write_csv(merged, out)
msg("✅ Wrote %s (%d rows, %d cols).", out, nrow(merged), ncol(merged))

