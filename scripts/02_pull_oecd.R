# scripts/02_pull_oecd.R
# Purpose: Pull & tidy OECD FUA Economy (city) and SME Finance (country)
# Cities: Tallinn (EE), Barcelona (ES), Athens (EL), Lisbon (PT)

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(stringr)
  library(vroom)
  library(rsdmx)   # parse SDMX endpoints + codelists
})

dir.create("data_raw",   showWarnings = FALSE)
dir.create("data_clean", showWarnings = FALSE)

# Increase connection buffer (OECD CSV headers can be huge)
Sys.setenv("VROOM_CONNECTION_SIZE" = 5e6)

# ---- 0) OECD SDMX URLs -------------------------------------------------------
URL_SME_FIN  <- "https://sdmx.oecd.org/public/rest/data/OECD.CFE.SMEE,DSD_SMEE_FINANCING@DF_SMEE_SCOREBOARD,1.0/all"
URL_REG_ECON <- "https://sdmx.oecd.org/public/rest/data/OECD.CFE.EDS,DSD_FUA_ECO@DF_ECONOMY,1.1/all"

# Fallback paths if you downloaded files manually
SME_LOCAL <- "data_raw/oecd_sme_fin_scoreboard_raw.csv"
FUA_LOCAL <- "data_raw/oecd_fua_economy_raw.csv"

city_map <- tibble::tibble(
  city         = c("Tallinn","Barcelona","Athens","Lisbon"),
  country      = c("Estonia","Spain","Greece","Portugal"),
  country_iso2 = c("EE","ES","EL","PT")
)

city_patterns <- list(
  Tallinn   = "(?i)Tallinn",
  Barcelona = "(?i)Barcelona",
  Athens    = "(?i)Athens|Attica|Attiki|Αττικ|Athina",
  Lisbon    = "(?i)Lisbon|Lisboa"
)

# ---------- helpers ----------
# Read SDMX and also extract a universal code->label lookup from codelists
read_sdmx_with_labels <- function(url) {
  sdmx <- rsdmx::readSDMX(url)
  df   <- as.data.frame(sdmx, labels = FALSE)   # keep raw codes
  # Build lookup of *all* codes -> labels from available codelists
  cl <- tryCatch(sdmx@codelists@codelists, error = function(e) NULL)
  lookups <- list()
  if (!is.null(cl) && length(cl)) {
    for (i in seq_along(cl)) {
      cli <- cl[[i]]
      # Some codelists have multilingual labels; prefer "en", fall back to first
      codes <- tryCatch(cli@codes, error = function(e) NULL)
      if (is.null(codes) || !length(codes)) next
      for (j in seq_along(codes)) {
        cj <- codes[[j]]
        idj <- tryCatch(cj@id, error = function(e) NA_character_)
        if (is.na(idj)) next
        # pick English label if present, else any first label
        labj <- NA_character_
        labs <- tryCatch(cj@labels, error = function(e) NULL)
        if (!is.null(labs) && length(labs)) {
          # labs is a named vector (e.g., c(en="Barcelona", fr="Barcelone"))
          if ("en" %in% names(labs)) labj <- as.character(labs[["en"]])
          if (is.na(labj)) labj <- as.character(labs[[1]])
        }
        lookups[[length(lookups) + 1L]] <- tibble(code = as.character(idj),
                                                  label = as.character(labj))
      }
    }
  }
  lookup_df <- if (length(lookups)) bind_rows(lookups) %>% distinct(code, .keep_all = TRUE) else NULL
  list(df = df, lookup = lookup_df)
}

# Decide whether to use SDMX or CSV and return list(df=..., lookup=...)
safe_read_url <- function(url, dest){
  if (!nzchar(url) || grepl("^PASTE_", url)) return(NULL)
  is_sdmx <- grepl("/rest/data/", url, fixed = TRUE)
  
  out <- tryCatch({
    if (is_sdmx) {
      read_sdmx_with_labels(url)
    } else {
      dat <- vroom::vroom(url, delim = ",", show_col_types = FALSE, .name_repair = "minimal")
      list(df = dat, lookup = NULL)
    }
  }, error = function(e) NULL)
  
  if (!is.null(out) && !is.null(out$df)) {
    # Snapshot the *data* to CSV for reproducibility (lookup is not saved)
    try(readr::write_csv(out$df, dest), silent = TRUE)
  }
  out
}

# Grep the first column whose name matches any of the patterns
col_pick <- function(nms, ...) {
  pats <- unlist(list(...))
  idx <- which(Reduce(`|`, lapply(pats, function(p) grepl(p, nms, ignore.case = TRUE))))
  if (length(idx)) nms[idx[1]] else NA_character_
}

# Safely extract a column by (matched) name
pluck_col <- function(df, pats, default = NA) {
  nm <- col_pick(names(df), pats)
  if (is.na(nm)) return(rep(default, nrow(df)))
  df[[nm]]
}

strip_accents <- function(x) {
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT")
  tolower(x)
}

# ------------------------- 1) SME Finance (country) ---------------------------
sme_res <- safe_read_url(URL_SME_FIN, SME_LOCAL)
if (is.null(sme_res) && file.exists(SME_LOCAL)) {
  sme_res <- list(df = vroom::vroom(SME_LOCAL, delim = ",", show_col_types = FALSE, .name_repair = "minimal"),
                  lookup = NULL)
}
if (is.null(sme_res) || is.null(sme_res$df)) stop("❌ SME Finance: provide a valid 'CSV with labels' URL or local file.")
sme_raw <- sme_res$df

sme_names <- names(sme_raw)
# Include obsTime variants and *.LABEL variants if present
col_country_code <- col_pick(sme_names, "^REF.?AREA$", "^LOCATION$", "country.?code", "ref_area")
col_country_lab  <- col_pick(sme_names, "REF.?AREA.*LABEL$", "^LOCATION.*LABEL$", "country.*label$", ".*\\.label$")
col_year         <- col_pick(sme_names, "^TIME.?PERIOD$", "^TIME$", "^OBSTIME$", "^OBS.?TIME$")
col_measure      <- col_pick(sme_names, "^MEASURE$", "indicator$", "series$", "concept$")
col_unit         <- col_pick(sme_names, "^UNIT.?MEASURE$", "^UNIT$")
col_prices       <- col_pick(sme_names, "^PRICES$")
col_value        <- col_pick(sme_names, "^OBS.?VALUE$", "^VALUE$", "^OBSVAL(UE)?$")

sme_keep <- tibble::tibble(
  country_iso_raw = toupper(as.character(pluck_col(sme_raw, col_country_code))),
  country_label   = as.character(if (!is.na(col_country_lab)) sme_raw[[col_country_lab]] else NA),
  year            = suppressWarnings(as.integer(pluck_col(sme_raw, col_year))),
  measure         = as.character(pluck_col(sme_raw, col_measure)),
  unit_measure    = as.character(if (!is.na(col_unit))  sme_raw[[col_unit]]  else NA),
  prices          = as.character(if (!is.na(col_prices)) sme_raw[[col_prices]] else NA),
  value           = suppressWarnings(as.numeric(pluck_col(sme_raw, col_value)))
)

# Normalise country codes to your 2-letter targets (EE, ES, EL, PT)
normalize_iso2 <- function(code, label) {
  c <- toupper(trimws(ifelse(is.na(code), "", code)))
  l <- tolower(trimws(ifelse(is.na(label), "", label)))
  dplyr::case_when(
    c %in% c("EE","EST")      | grepl("^estonia$", l)                           ~ "EE",
    c %in% c("ES","ESP")      | grepl("^spain|espa(ñ|n)a$", l)                  ~ "ES",
    c %in% c("EL","GR","GRC") | grepl("^greece|ελλάδα|ellada|hellas$", l)       ~ "EL",
    c %in% c("PT","PRT")      | grepl("^portugal$", l)                          ~ "PT",
    TRUE ~ c
  )
}

sme_keep <- sme_keep %>% mutate(country_iso = normalize_iso2(country_iso_raw, country_label))

message("SME distinct country codes (normalised): ",
        paste(sort(unique(na.omit(sme_keep$country_iso))), collapse = ", "))

sme_clean <- sme_keep %>%
  filter(country_iso %in% city_map$country_iso2, !is.na(year), !is.na(measure)) %>%
  left_join(city_map %>% distinct(country_iso2, country),
            by = c("country_iso" = "country_iso2")) %>%
  select(country_iso, country, year, measure, unit_measure, prices, value) %>%
  arrange(country, measure, year)

readr::write_csv(sme_clean, "data_clean/oecd_sme_fin_ee_es_el_pt.csv")
message("✅ Wrote data_clean/oecd_sme_fin_ee_es_el_pt.csv (", nrow(sme_clean), " rows)")

# ---------------------- 2) FUA Economy (city-level) ---------------------------
fua_res <- safe_read_url(URL_REG_ECON, FUA_LOCAL)
if (is.null(fua_res) && file.exists(FUA_LOCAL)) {
  fua_res <- list(df = vroom::vroom(FUA_LOCAL, delim = ",", show_col_types = FALSE, .name_repair = "minimal"),
                  lookup = NULL)
}
if (is.null(fua_res) || is.null(fua_res$df)) stop("❌ FUA Economy: provide a valid URL or local file.")

fua_raw <- fua_res$df
fua_lookup <- fua_res$lookup   # may be NULL if not SDMX

fua_names <- names(fua_raw)

# Likely columns (codes present; labels may be missing)
col_region_code <- col_pick(fua_names, "^REF.?AREA$", "^REGION$", "fua.?code", "region.*code")
col_region_lab  <- col_pick(fua_names,
                            "REF.?AREA.*LABEL$", "^REGION.*LABEL$", "^GEO.*LABEL$", "fua.*label$", "region.*label$", ".*\\.label$")
col_year        <- col_pick(fua_names, "^TIME.?PERIOD$", "^TIME$", "^OBSTIME$", "^OBS.?TIME$")
col_measure     <- col_pick(fua_names, "^MEASURE$", "indicator$", "series$", "concept$")
col_unit        <- col_pick(fua_names, "^UNIT.?MEASURE$", "^UNIT$")
col_tlv         <- col_pick(fua_names, "^TERRITORIAL.?LEVEL$", "^TL$","^TERRITORY.?LEVEL$")
col_value       <- col_pick(fua_names, "^OBS.?VALUE$", "^VALUE$", "^OBSVAL(UE)?$")

message("FUA matched columns → code: ", col_region_code,
        " | label: ", ifelse(is.na(col_region_lab), "NA", col_region_lab),
        " | year: ", ifelse(is.na(col_year), "NA", col_year),
        " | measure: ", col_measure,
        " | unit: ", col_unit,
        " | tlv: ", col_tlv,
        " | value: ", col_value)

fua_keep <- tibble::tibble(
  region_code     = as.character(pluck_col(fua_raw, col_region_code)),
  region_label    = as.character(pluck_col(fua_raw, col_region_lab)),
  year            = suppressWarnings(as.integer(pluck_col(fua_raw, col_year))),
  measure         = as.character(pluck_col(fua_raw, col_measure)),
  unit_measure    = as.character(if (!is.na(col_unit)) fua_raw[[col_unit]] else NA),
  territorial_lvl = as.character(if (!is.na(col_tlv))  fua_raw[[col_tlv]] else NA),
  value           = suppressWarnings(as.numeric(pluck_col(fua_raw, col_value)))
)

# If labels missing in the data frame, try to recover via SDMX codelists
if (all(is.na(fua_keep$region_label)) && !is.null(fua_lookup) && nrow(fua_lookup)) {
  # join on code -> label
  fua_keep <- fua_keep %>%
    left_join(fua_lookup %>% distinct(code, label), by = c("region_code" = "code")) %>%
    mutate(region_label = coalesce(region_label, label)) %>%
    select(-label)
  message("ℹ️  FUA: region_label recovered from SDMX codelists for ", sum(!is.na(fua_keep$region_label)), " rows.")
}

# If labels still NA, fall back to codes (so we can at least try to match)
if (all(is.na(fua_keep$region_label))) {
  message("⚠️  FUA: region_label not available; falling back to code matching.")
  fua_keep$region_label <- fua_keep$region_code
}

fua_keep <- fua_keep %>%
  filter(!is.na(region_label), !is.na(measure)) %>%
  # Accept any period format; if year is NA, keep the rows (we still want measures)
  filter(is.na(year) | !is.na(year))

# Filter to FUA rows if territorial level exists
if (!all(is.na(fua_keep$territorial_lvl))) {
  fua_keep <- fua_keep %>%
    filter(is.na(territorial_lvl) | grepl("FUA", territorial_lvl, ignore.case = TRUE))
}

# Normalise labels for robust city matching
fua_keep <- fua_keep %>%
  mutate(label_norm = strip_accents(region_label))

# Patterns (diacritics-insensitive)
pat_tallinn   <- "tallinn|ee0.*f"     # include likely code shapes as a fallback
pat_barcelona <- "barcelona|es0.*f"
pat_athens    <- "athens|attica|attiki|athina|gr0.*f"
pat_lisbon    <- "lisbon|lisboa|pt0.*f"

fua_four <- bind_rows(
  fua_keep %>% filter(str_detect(label_norm, pat_tallinn))   %>% mutate(city = "Tallinn"),
  fua_keep %>% filter(str_detect(label_norm, pat_barcelona)) %>% mutate(city = "Barcelona"),
  fua_keep %>% filter(str_detect(label_norm, pat_athens))    %>% mutate(city = "Athens"),
  fua_keep %>% filter(str_detect(label_norm, pat_lisbon))    %>% mutate(city = "Lisbon")
)

# If still no match, print a sample and stop
if (nrow(fua_four) == 0) {
  message("Sample distinct region labels/codes (first 30):")
  lbls <- fua_keep %>% distinct(region_label) %>% head(30) %>% pull(region_label)
  message(paste("  -", lbls, collapse = "\n"))
  stop("❌ After filtering, no FUA rows matched city labels/codes. We may need the exact FUA codes for the four cities.")
}

fua_clean <- fua_four %>%
  select(city, region_code, region_label, year, measure, unit_measure, value) %>%
  arrange(city, measure, year)

readr::write_csv(fua_clean, "data_clean/oecd_region_econom_barcelona_tallinn_athens_lisbon.csv")
message("✅ Wrote data_clean/oecd_region_econom_barcelona_tallinn_athens_lisbon.csv (", nrow(fua_clean), " rows)")

message("🏁 OECD pull complete.")



