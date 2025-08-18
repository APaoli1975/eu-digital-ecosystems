# scripts/03_build_indicators.R
# Purpose: Build harmonised KPIs for Tallinn, Barcelona, Athens, Lisbon
# Inputs:  data_clean/analysis_ready_4cities.csv  (from scripts 01 + 02)
# Outputs:
#   data_clean/kpis_all_long.csv
#   data_clean/kpis_robust_long.csv
#   data_clean/kpis_robust_wide_cityyear.csv
#   data_clean/kpis_scoreboard.csv
#   outputs/kpi_completeness_by_city.csv
#   outputs/kpi_eligibility.csv
#
# KPIs (ratio-based to reduce unit issues):
#   1) KPI_RnD_Intensity           = GERD_regional / GDP
#   2) KPI_HT_Employment_Share     = HighTech_Employment_regional / EMPW
#   3) KPI_HT_Patents_per_100kEmp  = (HighTech_patents_regional / EMPW) * 100,000
#   4) KPI_SBS_per_Emp             = SBS_hightech_regional / EMPW
#   5) KPI_HighGrowth_Share        = HighGrowth_enterprises_share_regional  (as-is)

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(tidyr); library(stringr)
  library(janitor); library(scales)
})

# ---------------- parameters (edit if needed) ----------------
year_min         <- 2010
year_max         <- 2024
min_completeness <- 0.70   # share of non-missing years within window, per city
min_years        <- 6      # minimum non-missing years per city within window
cities_expected  <- c("Tallinn","Barcelona","Athens","Lisbon")
# -------------------------------------------------------------

msg <- function(...) cat("[03-KPI] ", sprintf(...), "\n")
pct <- function(x) paste0(round(100 * x), "%")

# Ensure folders
dir.create("data_clean", showWarnings = FALSE)
dir.create("outputs",    showWarnings = FALSE)

# 0) Load master panel ---------------------------------------------------------
in_path <- "data_clean/analysis_ready_4cities.csv"
if (!file.exists(in_path)) stop("Missing file: ", in_path, call. = FALSE)

dat0 <- readr::read_csv(in_path, show_col_types = FALSE) %>%
  janitor::clean_names() %>%
  mutate(year = suppressWarnings(as.integer(year)))

req_cols <- c("city","indicator","year","value")
if (!all(req_cols %in% names(dat0))) {
  stop("Input is missing required columns: ", paste(setdiff(req_cols, names(dat0)), collapse=", "))
}

# Keep only the 4 case cities (safe if more are present)
dat0 <- dat0 %>% filter(city %in% cities_expected)

# 1) Pivot to wide so indicators become columns -------------------------------
ind_wide <- dat0 %>%
  select(city, year, indicator, value) %>%
  tidyr::pivot_wider(names_from = indicator, values_from = value)

msg("Indicators available after widening (first 20): %s",
    paste(head(names(ind_wide), 20), collapse=", "))

# ----- Safe column extractors (avoid referencing non-existent cols) ----------
get_col <- function(df, nm, default = NA_real_) {
  if (nm %in% names(df)) df[[nm]] else rep(default, nrow(df))
}

# Pull needed source series (as vectors, so mutate won't touch unknown columns)
GERD  <- get_col(ind_wide, "GERD_regional")
HTEMP <- get_col(ind_wide, "HighTech_Employment_regional")
HTPAT <- get_col(ind_wide, "HighTech_patents_regional")
SBS   <- get_col(ind_wide, "SBS_hightech_regional")
HGROW <- get_col(ind_wide, "HighGrowth_enterprises_share_regional")
GDP   <- get_col(ind_wide, "GDP")
EMPW  <- get_col(ind_wide, "EMPW")
LABP  <- get_col(ind_wide, "LAB_PROD")

# Fallback: if GDP missing but LAB_PROD and EMPW exist, compute GDP ≈ LAB_PROD * EMPW
if (all(is.na(GDP)) && (!all(is.na(LABP)) && !all(is.na(EMPW)))) {
  GDP <- ifelse(!is.na(LABP) & !is.na(EMPW), LABP * EMPW, NA_real_)
  msg("ℹ GDP approximated as LAB_PROD × EMPW (fallback).")
}

# Log availability
missing_cols <- c()
if (all(is.na(GDP)))  missing_cols <- c(missing_cols, "GDP")
if (all(is.na(EMPW))) missing_cols <- c(missing_cols, "EMPW")
if (length(missing_cols)) {
  msg("⚠ Source columns entirely missing (all NA): %s — KPIs depending on them will be NA.",
      paste(missing_cols, collapse=", "))
}

# 2) Build KPIs (guard denominators) ------------------------------------------
# Compute KPI vectors safely using the extracted vectors
KPI_RnD_Intensity <- ifelse(!is.na(GERD) & !is.na(GDP) & GDP != 0, GERD / GDP, NA_real_)
KPI_HT_Employment_Share <- ifelse(!is.na(HTEMP) & !is.na(EMPW) & EMPW != 0, HTEMP / EMPW, NA_real_)
KPI_HT_Patents_per_100kEmp <- ifelse(!is.na(HTPAT) & !is.na(EMPW) & EMPW > 0, (HTPAT / EMPW) * 1e5, NA_real_)
KPI_SBS_per_Emp <- ifelse(!is.na(SBS) & !is.na(EMPW) & EMPW > 0, SBS / EMPW, NA_real_)
KPI_HighGrowth_Share <- ifelse(!is.na(HGROW), HGROW, NA_real_)

kpi_df <- tibble::tibble(
  city = ind_wide$city,
  year = ind_wide$year,
  KPI_RnD_Intensity = KPI_RnD_Intensity,
  KPI_HT_Employment_Share = KPI_HT_Employment_Share,
  KPI_HT_Patents_per_100kEmp = KPI_HT_Patents_per_100kEmp,
  KPI_SBS_per_Emp = KPI_SBS_per_Emp,
  KPI_HighGrowth_Share = KPI_HighGrowth_Share
)

kpi_long <- kpi_df %>%
  tidyr::pivot_longer(-c(city, year), names_to = "KPI", values_to = "value") %>%
  arrange(KPI, city, year)

out_all_long <- "data_clean/kpis_all_long.csv"
readr::write_csv(kpi_long, out_all_long)
msg("Wrote %s (%s rows).", out_all_long, format(nrow(kpi_long), big.mark=","))

# 3) Apply robustness within window across ALL four cities --------------------
kpi_win <- kpi_long %>% filter(year >= year_min, year <= year_max)

comp <- kpi_win %>%
  group_by(KPI, city) %>%
  summarise(
    years_total  = n_distinct(year),
    years_non_na = n_distinct(year[!is.na(value)]),
    completeness = ifelse(years_total == 0, NA_real_, years_non_na / years_total),
    .groups = "drop"
  )

elig <- comp %>%
  group_by(KPI) %>%
  summarise(
    min_comp_all  = suppressWarnings(min(completeness, na.rm = TRUE)),
    min_years_all = suppressWarnings(min(years_non_na, na.rm = TRUE)),
    n_cities_seen = n_distinct(city),
    .groups = "drop"
  ) %>%
  mutate(included = is.finite(min_comp_all) & is.finite(min_years_all) &
           n_cities_seen == length(cities_expected) &
           min_comp_all >= min_completeness &
           min_years_all >= min_years)

robust_kpis <- elig %>% filter(included) %>% pull(KPI)

# If none pass, relax to 80% thresholds (explicit)
relaxed <- FALSE
if (!length(robust_kpis)) {
  relaxed <- TRUE
  elig <- elig %>%
    mutate(included_relax = is.finite(min_comp_all) & is.finite(min_years_all) &
             n_cities_seen == length(cities_expected) &
             min_comp_all >= (min_completeness * 0.8) &
             min_years_all >= max(3, floor(min_years * 0.8)))
  robust_kpis <- elig %>% filter(included_relax) %>% pull(KPI)
}

kpi_robust <- kpi_win %>% filter(KPI %in% robust_kpis)

# Save diagnostics
diag_comp <- "outputs/kpi_completeness_by_city.csv"
diag_elig <- "outputs/kpi_eligibility.csv"
readr::write_csv(comp, diag_comp)
readr::write_csv(elig, diag_elig)
msg("Wrote %s and %s.", diag_comp, diag_elig)

# Save robust panels (long + wide)
out_rob_long <- "data_clean/kpis_robust_long.csv"
readr::write_csv(kpi_robust, out_rob_long)
msg("Wrote %s (%s rows).", out_rob_long, format(nrow(kpi_robust), big.mark=","))

kpi_wide <- kpi_robust %>%
  tidyr::pivot_wider(names_from = KPI, values_from = value) %>%
  arrange(city, year)

out_rob_wide <- "data_clean/kpis_robust_wide_cityyear.csv"
readr::write_csv(kpi_wide, out_rob_wide)
msg("Wrote %s (%s rows, %s cols).", out_rob_wide, format(nrow(kpi_wide), big.mark=","), ncol(kpi_wide))

# 4) Scoreboard — last level & CAGR -------------------------------------------
calc_cagr <- function(y, v) {
  ok <- !is.na(v)
  if (sum(ok) < 2) return(NA_real_)
  years <- y[ok]; vals <- v[ok]
  t0 <- min(years); t1 <- max(years)
  v0 <- vals[which.min(years)]; v1 <- vals[which.max(years)]
  if (is.na(v0) || is.na(v1) || v0 <= 0 || t1 <= t0) return(NA_real_)
  (v1 / v0)^(1/(t1 - t0)) - 1
}

scoreboard <- kpi_robust %>%
  group_by(KPI, city) %>%
  summarise(
    first_year = suppressWarnings(min(year[!is.na(value)])),
    last_year  = suppressWarnings(max(year[!is.na(value)])),
    last_value = value[which.max(year * (!is.na(value)))],
    CAGR       = calc_cagr(year, value),
    obs_years  = n_distinct(year[!is.na(value)]),
    .groups = "drop"
  ) %>%
  arrange(KPI, desc(last_value)) %>%
  mutate(
    last_value_fmt = dplyr::case_when(
      grepl("Intensity|Share", KPI) ~ scales::percent(last_value, accuracy = 0.1),
      grepl("per_100kEmp", KPI)     ~ scales::comma(last_value, accuracy = 0.1),
      TRUE                          ~ scales::comma(last_value, accuracy = 1)
    ),
    CAGR_fmt = scales::percent(CAGR, accuracy = 0.1)
  )

out_score <- "data_clean/kpis_scoreboard.csv"
readr::write_csv(scoreboard, out_score)
msg("Wrote %s (%s rows).", out_score, format(nrow(scoreboard), big.mark=","))

# 5) Final console summary -----------------------------------------------------
msg("Window: %d–%d | Robustness: completeness ≥ %s & years ≥ %d (per city).",
    year_min, year_max, pct(min_completeness), min_years)
msg("Cities (expected): %s", paste(cities_expected, collapse=", "))
if (length(robust_kpis)) {
  msg("Robust KPIs: %s", paste(robust_kpis, collapse=", "))
} else {
  msg("No strict-robust KPIs; relaxed (80%%) applied → %s",
      ifelse(length(robust_kpis), paste(robust_kpis, collapse=", "), "still none"))
}
msg("🏁 KPI build complete.")



