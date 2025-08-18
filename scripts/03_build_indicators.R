# scripts/03_build_indicators.R
# --------------------------------------------------------------------
# KPI builder for 4 cities (Tallinn, Barcelona, Athens, Lisbon)
# - Robust KPIs (strict + extended) from Eurostat + OECD macro
# - NEW: "Descriptive" SME finance set (broad, non-robust evidence)
# - Auto-detects input columns, averages duplicates, interpolates
# --------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr); library(readr); library(tidyr); library(stringr); library(janitor); library(zoo)
})

msg <- function(...) cat("[03-KPI] ", sprintf(...), "\n")

# ============================ PARAMETERS =============================
CITIES <- c("Tallinn","Barcelona","Athens","Lisbon")

# "Fair" analysis window (aligns with OECD FUA macro)
ANALYSIS_YMIN <- 2000
ANALYSIS_YMAX <- 2021

# Robustness thresholds
STRICT_MIN_COMP  <- 0.70  # % completeness within window, per city
STRICT_MIN_YEARS <- 6
EXT_MIN_COMP     <- 0.50
EXT_MIN_YEARS    <- 4

# SME indicators (lowercase, after janitor::clean_names)
SME_CANDIDATES <- c(
  # general finance
  "bankruptcies","factoring","leasing","loan","loan_lt","loan_st","npl","payment_delays",
  # gov guarantees / direct
  "loan_gov_guaranteed","loan_gov_guarantees","loan_gov_direct",
  # credit conditions
  "interest_rate","interest_rate_spread","loan_rejection","loan_applications","loan_collateral",
  # venture capital (if present in SME set)
  "vc","venture_capital","venturecapital","venturecapital_pct_gdp_national"
)

# ============================ LOAD PANEL =============================
in_fp <- "data_clean/analysis_ready_4cities.csv"
if (!file.exists(in_fp)) stop("[03-KPI] ❌ Missing: ", in_fp, call. = FALSE)

dat0 <- read_csv(in_fp, show_col_types = FALSE) %>%
  clean_names() %>%
  filter(city %in% CITIES) %>%
  mutate(year = suppressWarnings(as.integer(year)))

# ======================= HELPERS (name detection) ====================
pick_col <- function(nms, patterns){
  for (p in patterns){
    hit <- nms[grepl(p, nms, ignore.case = TRUE)]
    if (length(hit)) return(hit[1])
  }
  NA_character_
}
get_num <- function(df, nm) if (!is.na(nm) && nm %in% names(df)) suppressWarnings(as.numeric(df[[nm]])) else rep(NA_real_, nrow(df))

# ================== DEDUP + PIVOT EUROSTAT INDICATORS ================
eu_agg <- dat0 %>%
  group_by(city, year, indicator) %>%
  summarise(value = mean(suppressWarnings(as.numeric(value)), na.rm = TRUE), .groups = "drop")

ind_wide <- eu_agg %>%
  tidyr::pivot_wider(names_from = indicator, values_from = value)

names(ind_wide) <- tolower(names(ind_wide))

# ===================== MACRO (GDP / EMPW / LAB_PROD) =================
nms0 <- names(dat0)
nm_gdp  <- pick_col(nms0, c("^gdp$","^gdp_"))
nm_empw <- pick_col(nms0, c("^empw$"))
nm_labp <- pick_col(nms0, c("^lab_?prod$"))

macro_cols <- unique(na.omit(c(nm_gdp, nm_empw, nm_labp)))
macro_wide <- if (length(macro_cols)) {
  dat0 %>%
    select(city, year, all_of(macro_cols)) %>%
    group_by(city, year) %>%
    summarise(across(everything(), ~ mean(suppressWarnings(as.numeric(.)), na.rm = TRUE)),
              .groups = "drop") %>%
    rename_with(tolower)
} else NULL

panel <- ind_wide
if (!is.null(macro_wide)) {
  panel <- panel %>% left_join(macro_wide, by = c("city","year"))
}
names(panel) <- tolower(names(panel))

msg("Columns after widening (first 20): %s", paste(head(names(panel), 20), collapse = ", "))

# ================ DETECT SERIES NEEDED FOR CORE KPIs =================
nms <- names(panel)
nm_gerd  <- pick_col(nms, c("^gerd", "rd.*exp", "r.?&.?d"))
nm_htemp <- pick_col(nms, c("^high.?tech.*employment","^htec.?_?emp","employment.*high.?tech"))
nm_htpat <- pick_col(nms, c("^high.?tech.*patent","^patent.*high.?tech","^pat.*_rtec$"))
nm_vc    <- pick_col(nms, c("^venture.*cap.*gdp","^vc.*gdp","^vc$","^venturecapital_pct_gdp_national$"))

nm_gdp_p  <- pick_col(nms, c("^gdp$","^gdp_"))
nm_empw_p <- pick_col(nms, c("^empw$"))
nm_labp_p <- pick_col(nms, c("^lab_?prod$"))

GERD  <- get_num(panel, nm_gerd)
HTEMP <- get_num(panel, nm_htemp)
HTPAT <- get_num(panel, nm_htpat)
VC    <- get_num(panel, nm_vc)

GDP   <- get_num(panel, nm_gdp_p)
EMPW  <- get_num(panel, nm_empw_p)
LABP  <- get_num(panel, nm_labp_p)

# GDP fallback via labour productivity × employment
if (all(is.na(GDP)) && any(!is.na(LABP) & !is.na(EMPW))) {
  GDP <- ifelse(!is.na(LABP) & !is.na(EMPW), LABP * EMPW, NA_real_)
  msg("ℹ GDP approximated as LAB_PROD × EMPW (fallback).")
}

# ========================== BUILD CORE KPIs ==========================
kpi_df <- tibble(
  city = panel$city,
  year = panel$year,
  kpi_ht_patents_per_100kemp = ifelse(!is.na(HTPAT) & !is.na(EMPW) & EMPW > 0, (HTPAT/EMPW)*1e5, NA_real_),
  kpi_ht_employment_share    = ifelse(!is.na(HTEMP) & !is.na(EMPW) & EMPW > 0, (HTEMP/EMPW)*100,  NA_real_),
  kpi_vc_intensity           = ifelse(!is.na(VC), VC, NA_real_),
  kpi_rnd_intensity          = ifelse(!is.na(GERD) & !is.na(GDP) & GDP > 0, (GERD/GDP)*100, NA_real_)
)

kpi_long <- kpi_df %>%
  pivot_longer(-c(city, year), names_to = "indicator", values_to = "value") %>%
  group_by(city, indicator) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(value_interp = zoo::na.approx(value, year, na.rm = FALSE)) %>%
  ungroup()

# -------- Restrict to fair window for robustness (not for descriptive) --------
kpi_win <- kpi_long %>% filter(year >= ANALYSIS_YMIN, year <= ANALYSIS_YMAX)

write_csv(kpi_win, "data_clean/kpis_all_long.csv")
msg("Wrote data_clean/kpis_all_long.csv (%s rows) within window %d–%d.",
    format(nrow(kpi_win), big.mark=","), ANALYSIS_YMIN, ANALYSIS_YMAX)

# ====================== ROBUSTNESS (strict & extended) =======================
comp_tbl <- kpi_win %>%
  group_by(indicator, city) %>%
  summarise(
    years_total  = n_distinct(year),
    years_non_na = n_distinct(year[!is.na(value_interp)]),
    completeness = ifelse(years_total > 0, years_non_na / years_total, 0),
    .groups = "drop"
  )
write_csv(comp_tbl, "outputs/kpi_completeness_by_city.csv")

eligible_all4 <- comp_tbl %>%
  group_by(indicator) %>%
  summarise(
    min_comp_all  = min(completeness, na.rm = TRUE),
    min_years_all = min(years_non_na, na.rm = TRUE),
    .groups = "drop"
  )

elig_strict <- eligible_all4 %>%
  mutate(included = min_comp_all >= STRICT_MIN_COMP & min_years_all >= STRICT_MIN_YEARS)

eligible_any2 <- comp_tbl %>%
  group_by(indicator) %>%
  summarise(n_cities_good = sum(completeness >= EXT_MIN_COMP & years_non_na >= EXT_MIN_YEARS),
            .groups = "drop")

elig_extended <- eligible_any2 %>%
  mutate(included = n_cities_good >= 2)

inds_strict   <- elig_strict   %>% filter(included) %>% pull(indicator)
inds_extended <- elig_extended %>% filter(included) %>% pull(indicator)

kpi_strict   <- kpi_win %>% filter(indicator %in% inds_strict)
kpi_extended <- kpi_win %>% filter(indicator %in% inds_extended)

write_csv(kpi_strict,   "data_clean/kpis_strict_long.csv")
write_csv(kpi_extended, "data_clean/kpis_extended_long.csv")

kpi_strict %>%
  pivot_wider(names_from = indicator, values_from = value_interp) %>%
  arrange(city, year) %>%
  write_csv("data_clean/kpis_strict_wide_cityyear.csv")

# ==================== SME FINANCE — DESCRIPTIVE SET ===================
# We want maximum evidence. We DON'T restrict to the fair window.
# 1) Identify SME finance columns present in the merged panel.
panel_all_rows <- dat0  # use the full, non-windowed data with possible repetition

# Average to city-year to remove duplication (Eurostat long structure)
sme_cols_present <- intersect(SME_CANDIDATES, names(panel_all_rows))
sme_descriptive <- NULL

if (length(sme_cols_present)) {
  sme_cityyear <- panel_all_rows %>%
    select(city, year, all_of(sme_cols_present)) %>%
    group_by(city, year) %>%
    summarise(across(everything(), ~ {
      v <- suppressWarnings(as.numeric(.))
      out <- mean(v, na.rm = TRUE)
      if (is.nan(out)) NA_real_ else out
    }), .groups = "drop")
  
  # LONG form
  sme_descriptive <- sme_cityyear %>%
    pivot_longer(cols = -c(city, year), names_to = "indicator", values_to = "value") %>%
    arrange(indicator, city, year) %>%
    group_by(city, indicator) %>%
    mutate(value_interp = zoo::na.approx(value, year, na.rm = FALSE)) %>%
    ungroup() %>%
    mutate(source = "OECD_SME_finance", quality = "descriptive")
}

if (!is.null(sme_descriptive)) {
  write_csv(sme_descriptive, "data_clean/sme_descriptive_long.csv")
  # Coverage table for SME descriptive
  sme_comp <- sme_descriptive %>%
    group_by(indicator, city) %>%
    summarise(
      years_total  = n_distinct(year),
      years_non_na = n_distinct(year[!is.na(value)]),
      completeness = ifelse(years_total > 0, years_non_na / years_total, 0),
      .groups = "drop"
    )
  write_csv(sme_comp, "outputs/sme_descriptive_coverage_by_city.csv")
  msg("Wrote data_clean/sme_descriptive_long.csv (%s rows).", format(nrow(sme_descriptive), big.mark=","))
} else {
  msg("ℹ No SME finance columns found among: %s", paste(SME_CANDIDATES, collapse=", "))
}

# ============================ SCOREBOARD =============================
# KPI scoreboard (strict/extended)
scoreboard <- comp_tbl %>%
  mutate(
    pass_strict   = completeness >= STRICT_MIN_COMP & years_non_na >= STRICT_MIN_YEARS,
    pass_extended = completeness >= EXT_MIN_COMP    & years_non_na >= EXT_MIN_YEARS
  ) %>%
  select(indicator, city, pass_strict, pass_extended) %>%
  pivot_longer(cols = c(pass_strict, pass_extended), names_to = "rule", values_to = "pass") %>%
  mutate(rule = ifelse(rule == "pass_strict", "strict", "extended")) %>%
  pivot_wider(names_from = city, values_from = pass) %>%
  mutate(across(-c(indicator, rule), ~ ifelse(.x, "✓", "✗")))
write_csv(scoreboard, "outputs/kpi_scoreboard.csv")

msg("Strict set:   %s rows (%s indicators).", format(nrow(kpi_strict), big.mark=","), length(inds_strict))
msg("Extended set: %s rows (%s indicators).", format(nrow(kpi_extended), big.mark=","), length(inds_extended))
msg("Strict indicators:   %s", ifelse(length(inds_strict), paste(inds_strict, collapse=", "), "none"))
msg("Extended indicators: %s", ifelse(length(inds_extended), paste(inds_extended, collapse=", "), "none"))
msg("🏁 KPI build complete.")


