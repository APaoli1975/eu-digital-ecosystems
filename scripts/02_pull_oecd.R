# ---- 2) SME Finance (country-level) -----------------------------------------
fp_sme <- "data_raw/oecd_sme_fin_scoreboard_raw.csv"
if (!file.exists(fp_sme)) {
  msg("ℹ SME finance file not found at %s — skipping SME step.", fp_sme)
} else {
  msg("Reading SME finance from %s ...", fp_sme)
  sme_raw <- suppressMessages(
    vroom::vroom(fp_sme, delim = ",", show_col_types = FALSE, .name_repair = "minimal")
  )
  if (!nrow(sme_raw)) {
    msg("⚠ SME file is empty. Skipping.")
  } else {
    nms2 <- names(sme_raw)
    
    # Flexible column detection
    pick <- function(...) {
      pats <- unlist(list(...))
      hit <- nms2[Reduce(`|`, lapply(pats, function(p) grepl(p, nms2, ignore.case = TRUE)))]
      if (length(hit)) hit[1] else NA_character_
    }
    col_ref_area <- pick("^REF.?AREA$", "^geo$", "country.?code")
    col_label    <- pick("REF.?AREA.*LABEL", "country.*label", "^country$")
    col_year     <- pick("^TIME.?PERIOD$", "^obsTime$", "^TIME$", "^year$")
    col_measure  <- pick("^MEASURE$", "indicator", "series", "concept", "measure.*code")
    col_unit     <- pick("^UNIT.?MEASURE$", "^UNIT$")
    col_value    <- pick("^OBS.?VALUE$", "^obsValue$", "^VALUE$")
    
    if (is.na(col_ref_area) || is.na(col_year) || is.na(col_measure) || is.na(col_value)) {
      stop("[02] SME: Could not detect essential columns. Found: ", paste(nms2, collapse=", "))
    }
    
    sme_keep <- tibble::tibble(
      ref_area_raw = as.character(sme_raw[[col_ref_area]]),
      country_lab  = as.character(if (!is.na(col_label)) sme_raw[[col_label]] else NA),
      year         = suppressWarnings(as.integer(sme_raw[[col_year]])),
      measure      = as.character(sme_raw[[col_measure]]),
      unit_measure = as.character(if (!is.na(col_unit)) sme_raw[[col_unit]] else NA),
      value        = suppressWarnings(as.numeric(sme_raw[[col_value]]))
    )
    
    # Normalize to ISO-2 for our 4 targets (handle EL/GR/GRC)
    to_iso2 <- function(code, lab) {
      x <- toupper(coalesce(code, ""))
      y <- toupper(coalesce(lab,  ""))
      
      dplyr::case_when(
        x %in% c("EE","EST")               ~ "EE",
        x %in% c("ES","ESP")               ~ "ES",
        x %in% c("EL","GR","GRC")          ~ "EL",      # Greece
        x %in% c("PT","PRT")               ~ "PT",
        
        grepl("\\bESTONIA\\b",   y)        ~ "EE",
        grepl("\\bSPAIN\\b|ESPA", y)       ~ "ES",
        grepl("\\bGREECE\\b|HELLENIC|ELL", y) ~ "EL",
        grepl("\\bPORTUGAL\\b",  y)        ~ "PT",
        
        TRUE ~ NA_character_
      )
    }
    
    sme_keep <- sme_keep %>%
      mutate(
        country_iso = to_iso2(ref_area_raw, country_lab),
        country     = case_when(
          country_iso == "EE" ~ "Estonia",
          country_iso == "ES" ~ "Spain",
          country_iso == "EL" ~ "Greece",
          country_iso == "PT" ~ "Portugal",
          TRUE ~ NA_character_
        )
      ) %>%
      filter(!is.na(country_iso), !is.na(year), !is.na(measure), !is.na(value))
    
    msg("SME distinct country codes (normalized): %s",
        paste(sort(unique(sme_keep$country_iso)), collapse = ", "))
    
    # Keep only our four countries
    sme_clean <- sme_keep %>%
      filter(country_iso %in% c("EE","ES","EL","PT")) %>%
      select(country_iso, country, year, measure, unit_measure, value) %>%
      arrange(country, measure, year)
    
    out_sme <- "data_clean/oecd_sme_fin_ee_es_el_pt.csv"
    readr::write_csv(sme_clean, out_sme)
    msg("✅ Wrote %s (%s rows).", out_sme, format(nrow(sme_clean), big.mark = ","))
  }
}
