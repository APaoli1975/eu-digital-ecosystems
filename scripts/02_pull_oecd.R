# scripts/02_pull_oecd.R
# Purpose: Pull & tidy OECD FUA Economy (city) and SME Finance (country)
# Cities: Tallinn (EE), Barcelona (ES), Athens (EL), Lisbon (PT)

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(stringr)
  library(vroom)
})

dir.create("data_raw",   showWarnings = FALSE)
dir.create("data_clean", showWarnings = FALSE)

msg <- function(...) cat("[02] ", sprintf(...), "\n")

# Increase connection buffer (OECD CSV headers can be huge)
Sys.setenv("VROOM_CONNECTION_SIZE" = 5e6)

# ---- Input files (already downloaded) ---------------------------------------
# Your uploaded filenames:
fp_fua <- "data_raw/oecd_fua_economy_raw.csv"    # SDMX CSV (with labels ideally)
fp_sme <- "data_raw/oecd_sme_fin_scoreboard_raw.csv"  # optional; else skip SME

if (!file.exists(fp_fua)) {
  stop("[02] ❌ FUA Economy file not found: ", fp_fua,
       "\nPlace your OECD 'CSV with labels' export at that path.")
}

# ---- Helpers ----------------------------------------------------------------
col_pick <- function(nms, ...) {
  pats <- unlist(list(...))
  idx <- which(Reduce(`|`, lapply(pats, function(p) grepl(p, nms, ignore.case = TRUE))))
  if (length(idx)) nms[idx[1]] else NA_character_
}

pluck_col <- function(df, pats, default = NA) {
  nm <- col_pick(names(df), pats)
  if (is.na(nm)) return(rep(default, nrow(df)))
  df[[nm]]
}

# ---- 1) FUA Economy (city-level) --------------------------------------------
msg("Reading FUA economy from %s ...", fp_fua)
fua_raw <- suppressMessages(vroom::vroom(fp_fua, delim = ",", show_col_types = FALSE, .name_repair = "minimal"))

if (!nrow(fua_raw)) stop("[02] ❌ Empty FUA file: ", fp_fua)

# Detect typical SDMX columns
nms <- names(fua_raw)
col_region_code <- col_pick(nms, "^REF.?AREA$", "region.*code", "geo(code)?")
col_region_lab  <- col_pick(nms, "REF.?AREA.*LABEL", "region.*label")
col_year        <- col_pick(nms, "^TIME.?PERIOD$", "^obsTime$", "^TIME$", "year")
col_measure     <- col_pick(nms, "^MEASURE$", "indicator", "series", "concept", "measure.*code")
col_unit        <- col_pick(nms, "^UNIT.?MEASURE$", "^UNIT$")
col_tlv         <- col_pick(nms, "^TERRITORIAL.?LEVEL$", "TLV")
col_value       <- col_pick(nms, "^OBS.?VALUE$", "^obsValue$", "^VALUE$")

msg("FUA matched columns → code: %s | label: %s | year: %s | measure: %s | unit: %s | tlv: %s | value: %s",
    col_region_code %||% "NA", col_region_lab %||% "NA", col_year %||% "NA", col_measure %||% "NA",
    col_unit %||% "NA", col_tlv %||% "NA", col_value %||% "NA")

fua_keep <- tibble::tibble(
  region_code     = as.character(pluck_col(fua_raw, col_region_code)),
  region_label    = as.character(pluck_col(fua_raw, col_region_lab)),
  year            = suppressWarnings(as.integer(pluck_col(fua_raw, col_year))),
  measure         = as.character(pluck_col(fua_raw, col_measure)),
  unit_measure    = as.character(if (!is.na(col_unit)) fua_raw[[col_unit]] else NA),
  territorial_lvl = as.character(if (!is.na(col_tlv))  fua_raw[[col_tlv]] else NA),
  value           = suppressWarnings(as.numeric(pluck_col(fua_raw, col_value)))
) %>%
  filter(!is.na(region_code), !is.na(measure), !is.na(value))

# Prefer actual FUA rows if territorial level is present
if (!all(is.na(fua_keep$territorial_lvl))) {
  fua_keep <- fua_keep %>%
    mutate(territorial_lvl = toupper(territorial_lvl)) %>%
    filter(is.na(territorial_lvl) | territorial_lvl == "FUA")
}

# Map OECD region_code → our city names
fua_map <- tibble::tribble(
  ~region_code, ~city,
  "EE001F", "Tallinn",
  "ES002F", "Barcelona",
  "EL001F", "Athens",
  "PT001F", "Lisbon"
)

# If label column missing, list a few codes for manual verification
if (all(is.na(fua_keep$region_label))) {
  msg("⚠ FUA: region_label not available; matching on region_code only.")
}

# Filter to our 4 FUAs and attach city
fua_four <- fua_keep %>%
  inner_join(fua_map, by = "region_code")

if (!nrow(fua_four)) {
  stop("[02] ❌ After mapping, no FUA rows matched EE001F/ES002F/EL001F/PT001F.\n",
       "Check your file's region_code values; print a sample like:\n",
       "unique(head(fua_keep$region_code, 50))")
}

# sanity: list first measures
meas_sample <- sort(unique(fua_four$measure))
msg("FUA unique measures (first 20): %s",
    paste(head(meas_sample, 20), collapse = ", "))

# Keep only macro measures we need downstream; adjust set if your file uses variants
# Common names seen in OECD FUA Economy exports: "GDP", "GDPPC", "LAB_PROD", "EMPW"
keep_measures <- c("GDP", "GDPPC", "LAB_PROD", "EMPW")
fua_filtered <- fua_four %>%
  filter(measure %in% keep_measures) %>%
  select(city, region_code, region_label, year, measure, unit_measure, value) %>%
  arrange(city, measure, year)

# Write the long file used by the previous pipeline/03 script
out_fua <- "data_clean/oecd_region_econom_barcelona_tallinn_athens_lisbon.csv"
readr::write_csv(fua_filtered, out_fua)
msg("✅ Wrote %s (%s rows).", out_fua, format(nrow(fua_filtered), big.mark=","))

# ---- 2) SME Finance (country-level) -----------------------------------------
# Optional: only if you also want SME finance in the pipeline
if (file.exists(fp_sme)) {
  msg("Reading SME finance from %s ...", fp_sme)
  sme_raw <- suppressMessages(vroom::vroom(fp_sme, delim = ",", show_col_types = FALSE, .name_repair = "minimal"))
  
  # Try flexible column detection
  nms2 <- names(sme_raw)
  col_country_code <- col_pick(nms2, "^REF.?AREA$", "country.?code", "ref_area", "^geo$")
  col_country_lab  <- col_pick(nms2, "REF.?AREA.*LABEL", "country.*label", "country")
  col_year         <- col_pick(nms2, "^TIME.?PERIOD$", "^obsTime$", "^TIME$", "year")
  col_measure      <- col_pick(nms2, "^MEASURE$", "indicator", "series", "concept")
  col_unit         <- col_pick(nms2, "^UNIT.?MEASURE$", "^UNIT$")
  col_prices       <- col_pick(nms2, "^PRICES$")
  col_value        <- col_pick(nms2, "^OBS.?VALUE$", "^obsValue$", "^VALUE$")
  
  sme_keep <- tibble::tibble(
    country_iso = toupper(as.character(pluck_col(sme_raw, col_country_code))),
    country     = as.character(if (!is.na(col_country_lab)) sme_raw[[col_country_lab]] else NA),
    year        = suppressWarnings(as.integer(pluck_col(sme_raw, col_year))),
    measure     = as.character(pluck_col(sme_raw, col_measure)),
    unit_measure= as.character(if (!is.na(col_unit))  sme_raw[[col_unit]]  else NA),
    prices      = as.character(if (!is.na(col_prices)) sme_raw[[col_prices]] else NA),
    value       = suppressWarnings(as.numeric(pluck_col(sme_raw, col_value)))
  )
  
  # Four countries only
  wanted_iso2 <- c("EE","ES","EL","PT")
  # If country label missing, map via iso
  country_map <- tibble::tibble(
    country_iso = wanted_iso2,
    country     = c("Estonia","Spain","Greece","Portugal")
  )
  
  sme_clean <- sme_keep %>%
    filter(country_iso %in% wanted_iso2, !is.na(year), !is.na(measure)) %>%
    left_join(country_map, by = "country_iso") %>%
    mutate(country = coalesce(country.x, country.y)) %>%
    select(country_iso, country, year, measure, unit_measure, prices, value) %>%
    arrange(country, measure, year)
  
  out_sme <- "data_clean/oecd_sme_fin_ee_es_el_pt.csv"
  readr::write_csv(sme_clean, out_sme)
  msg("✅ Wrote %s (%s rows).", out_sme, format(nrow(sme_clean), big.mark=","))
} else {
  msg("ℹ SME finance file not found at %s — skipping SME step.", fp_sme)
}

msg("🏁 OECD pull complete.")


