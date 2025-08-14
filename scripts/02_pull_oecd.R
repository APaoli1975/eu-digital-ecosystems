# scripts/02_pull_oecd.R
# Robust OECD SDMX pull: RAW parsed + CLEAN filtered outputs.

# ===== 0) CONFIG =====
# Paste your OECD SDMX "Data query" URLs (from Data Explorer → Developer API → Data query)
URL_SME_FIN  <- "https://sdmx.oecd.org/public/rest/data/OECD.CFE.SMEE,DSD_SMEE_FINANCING@DF_SMEE_SCOREBOARD,1.0/all"
URL_REG_ECON <- "https://sdmx.oecd.org/public/rest/data/OECD.CFE.EDS,DSD_FUA_ECO@DF_ECONOMY,1.1/all"

# Filter targets
COUNTRIES_KEEP    <- c("EST","ESP")          # SME finance (country-level)
REGIONS_KEEP_FUA  <- c("ES002F","EE001F")    # Preferred (FUA): Barcelona, Tallinn
REGIONS_KEEP_TL2  <- c("ES51","EE00")        # Fallback (TL2): Catalonia, Estonia TL2

# Controls
REFRESH <- TRUE    # FALSE = reuse *_latest.csv if present (faster re-runs)
VERBOSE <- FALSE   # TRUE  = print progress messages

# ===== 1) SETUP =====
quiet_pkgs <- c("rsdmx","dplyr","readr","janitor","stringr","fs")
missing <- setdiff(quiet_pkgs, rownames(installed.packages()))
if (length(missing)) install.packages(missing, dependencies = TRUE)

suppressPackageStartupMessages({
  library(rsdmx)
  library(dplyr)
  library(readr)
  library(janitor)
  library(stringr)
  library(fs)
})

dir_create("data_raw"); dir_create("data_clean")
timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
log <- function(...) if (isTRUE(VERBOSE)) message(...)

# ===== 2) HELPERS =====

# Clean & force unique, snake_case names
make_unique_names <- function(x) {
  x <- janitor::make_clean_names(x, case = "snake")
  if (any(duplicated(x))) {
    dup <- duplicated(x) | duplicated(x, fromLast = TRUE)
    x[dup] <- paste0(x[dup], "_", ave(seq_along(x)[dup], x[dup], FUN = seq_along))
  }
  x
}

read_sdmx_df <- function(url) {
  stopifnot(nzchar(url))
  obj <- rsdmx::readSDMX(url)
  df  <- as.data.frame(obj)
  names(df) <- make_unique_names(names(df))
  df
}

write_raw_with_latest <- function(df, stem) {
  raw_ts     <- file.path("data_raw", paste0(stem, "_", timestamp, ".csv"))
  raw_latest <- file.path("data_raw", paste0(stem, "_latest.csv"))
  readr::write_csv(df, raw_ts)
  readr::write_csv(df, raw_latest)
  log("RAW → ", raw_ts, " & ", raw_latest)
  invisible(raw_latest)
}

reuse_or_pull <- function(stem, url, normaliser) {
  latest <- file.path("data_raw", paste0(stem, "_latest.csv"))
  if (!REFRESH && file.exists(latest)) {
    log("Reusing ", latest)
    df <- suppressMessages(readr::read_csv(latest, show_col_types = FALSE))
    names(df) <- make_unique_names(names(df))
    return(normaliser(df))
  } else {
    log("Pulling SDMX: ", url)
    df <- read_sdmx_df(url)
    df <- normaliser(df)
    write_raw_with_latest(df, stem)
    return(df)
  }
}

# Normalise core columns safely (no duplicate target names)
normalise_cols <- function(df) {
  nm <- names(df)
  
  # country_iso3 (ISO3 or FUA code depending on dataset)
  if (!"country_iso3" %in% nm) {
    if ("ref_area" %in% nm)        df <- dplyr::rename(df, country_iso3 = ref_area)  else
      if ("location" %in% nm)        df <- dplyr::rename(df, country_iso3 = location)  else
        if ("country"  %in% nm)        df <- dplyr::rename(df, country_iso3 = country)   else
          df$country_iso3 <- NA_character_
  }
  
  # region_code (for TL2/TL3/FUA IDs)
  if (!"region_code" %in% names(df)) {
    if ("region"    %in% nm)       df <- dplyr::rename(df, region_code = region)     else
      if ("ref_area2" %in% nm)       df <- dplyr::rename(df, region_code = ref_area2)  else
        df$region_code <- NA_character_
  }
  
  # year (keep character to allow A/Q/M)
  if (!"year" %in% names(df)) {
    if ("time_period" %in% nm)     df <- dplyr::rename(df, year = time_period)       else
      if ("time"        %in% nm)     df <- dplyr::rename(df, year = time)              else
        if ("obs_time"    %in% nm)     df <- dplyr::rename(df, year = obs_time)          else
          if ("obstime"     %in% nm)     df <- dplyr::rename(df, year = obstime)           else
            df$year <- NA_character_
  }
  df$year <- as.character(df$year)
  
  # value (numeric)
  if (!"value" %in% names(df)) {
    if ("obs_value" %in% nm)       df <- dplyr::rename(df, value = obs_value)        else
      df$value <- NA_real_
  }
  suppressWarnings(df$value <- as.numeric(df$value))
  
  df
}

# Regional special: promote FUA code to region_code when territorial_level indicates FUA
normalise_cols_reg <- function(df) {
  df <- normalise_cols(df)
  nm <- names(df)
  if (!"territorial_level" %in% nm) df$territorial_level <- NA_character_
  if (!"region_code" %in% names(df)) df$region_code <- NA_character_
  if ("country_iso3" %in% names(df)) {
    df <- df %>%
      dplyr::mutate(
        region_code = dplyr::if_else(
          territorial_level %in% c("FUA","FUA_TL"),
          country_iso3, region_code, region_code
        )
      )
  }
  df
}

# ===== 3) SME FINANCE (country-level: EST, ESP) =====
sme_raw <- reuse_or_pull("oecd_sme_finance_parsed", URL_SME_FIN, normalise_cols)

if ("country_iso3" %in% names(sme_raw)) {
  sme_clean <- sme_raw %>%
    dplyr::filter(country_iso3 %in% COUNTRIES_KEEP) %>%
    dplyr::arrange(country_iso3, year)
  readr::write_csv(sme_clean, "data_clean/oecd_sme_fin_ee_es.csv")
  log("CLEAN → data_clean/oecd_sme_fin_ee_es.csv")
} else {
  message("⚠️ SME finance: no usable country column. Inspect data_raw/oecd_sme_finance_parsed_latest.csv")
}

# ===== 4) REGIONAL ECONOMY (prefer FUA; fallback TL2; fallback labels) =====
reg_raw <- reuse_or_pull("oecd_regional_economy_parsed", URL_REG_ECON, normalise_cols_reg)

# Best-effort: join FUA labels (ignore errors if OECD file not reachable)
join_fua_labels <- function(df) {
  tryCatch({
    map <- readr::read_csv(
      "https://www.oecd.org/content/dam/oecd/en/data/datasets/oecd-definition-of-cities-and-functional-urban-areas/list_of_municipalities_in_FUAs_and_Cities.csv",
      show_col_types = FALSE
    ) %>%
      janitor::clean_names() %>%
      dplyr::select(fua_code = dplyr::any_of(c("fua_code","FUA_CODE")),
                    fua_name = dplyr::any_of(c("fua_name","FUA_NAME"))) %>%
      dplyr::distinct() %>%
      dplyr::filter(!is.na(fua_code))
    df %>%
      dplyr::left_join(map, by = c("region_code" = "fua_code")) %>%
      dplyr::mutate(region_label = dplyr::coalesce(.data$fua_name, .data$region_label))
  }, error = function(e) df)
}
reg_raw <- join_fua_labels(reg_raw)

# Ensure guards (so filters don't error if label missing)
if (!"region_code"  %in% names(reg_raw)) reg_raw$region_code  <- NA_character_
if (!"region_label" %in% names(reg_raw)) reg_raw$region_label <- NA_character_

# 1) Try FUA codes first
reg_keep <- reg_raw %>%
  dplyr::filter(region_code %in% REGIONS_KEEP_FUA) %>%
  dplyr::arrange(region_code, year)

# 2) If no FUA rows, try TL2 codes (ES51/EE00)
if (nrow(reg_keep) == 0) {
  reg_keep <- reg_raw %>%
    dplyr::filter(region_code %in% REGIONS_KEEP_TL2) %>%
    dplyr::arrange(region_code, year)
}

# 3) If still empty, fallback to label matching (Barcelona|Tallinn)
if (nrow(reg_keep) == 0 && any(!is.na(reg_raw$region_label))) {
  reg_keep <- reg_raw %>%
    dplyr::filter(stringr::str_detect(dplyr::coalesce(region_label, ""),
                                      stringr::regex("Barcelona|Tallinn", ignore_case = TRUE))) %>%
    dplyr::arrange(region_label, year)
}

readr::write_csv(reg_keep, "data_clean/oecd_region_econom_barcelona_tallinn.csv")
log("CLEAN → data_clean/oecd_region_econom_barcelona_tallinn.csv")

cat("🏁 OECD SDMX pull complete.\n")