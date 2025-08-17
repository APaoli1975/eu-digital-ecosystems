# scripts/01_pull_eurostat.R
# Purpose: Pull Eurostat indicators for Tallinn, Barcelona, Athens/Attica, Lisbon/AML.
# Outputs per-city CSV + XLSX in data_clean/.

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tidyr)
  library(stringr)
  library(janitor)
  library(lubridate)
  library(purrr)
  library(eurostat)
  library(writexl)
})

# ---------- Folders ----------
dir.create("data_raw",   showWarnings = FALSE)
dir.create("data_clean", showWarnings = FALSE)

# ---------- Targets & flexible NUTS matching ----------
targets <- tibble::tibble(
  city              = c("Tallinn","Barcelona","Athens","Lisbon"),
  nuts3_exact       = c("EE001",   "ES511",    "EL305|EL30", "PT170|PT17"),
  nuts2_parent      = c("EE00",    "ES51",     "EL30",       "PT17"),
  region_label_canon= c("Põhja-Eesti (EE001)","Barcelona (ES511)","Attiki (EL30)","Área Metropolitana de Lisboa (PT17)")
) %>%
  mutate(nuts_pattern = paste0("^(", nuts3_exact, ")$|^(", nuts2_parent, ")"))

# ---------- Indicator list ----------
tbl_gerd      <- "rd_e_gerdreg"   # regional
tbl_htec_emp  <- "htec_emp_reg2"  # regional
tbl_patents   <- "pat_ep_rtec"    # regional
tbl_sbs       <- "sbs_na_ind_r2"  # often national in practice
tbl_highgrow  <- "bd_9pm_r2"      # often national in practice
tbl_vc_nat    <- "tec00040"       # national

tables <- c(tbl_gerd, tbl_htec_emp, tbl_patents, tbl_sbs, tbl_highgrow, tbl_vc_nat)

message("Pulling Eurostat tables (cached if already downloaded)...")

# ---------- Helpers ----------
robust_year <- function(x) {
  if (inherits(x, "Date"))    return(lubridate::year(x))
  if (inherits(x, "yearmon")) return(as.integer(floor(as.numeric(x))))
  if (is.numeric(x))          return(as.integer(x))
  if (is.character(x))        return(suppressWarnings(as.integer(substr(x, 1L, 4L))))
  suppressWarnings(as.integer(x))
}

safe_min <- function(x) if (length(x) == 0 || all(is.na(x))) NA_integer_ else suppressWarnings(min(x, na.rm = TRUE))
safe_max <- function(x) if (length(x) == 0 || all(is.na(x))) NA_integer_ else suppressWarnings(max(x, na.rm = TRUE))

# Standardise core columns (keep codes; attach labels when available)
norm_one <- function(df, code){
  df <- df %>% clean_names()
  geo  <- if ("geo" %in% names(df)) df$geo else if ("geo_code" %in% names(df)) df$geo_code else NA
  time_col <- if ("time" %in% names(df)) df$time else if ("year" %in% names(df)) df$year else NA
  year <- robust_year(time_col)
  val  <- if ("values" %in% names(df)) df$values else if ("value" %in% names(df)) df$value else NA_real_
  
  tibble::tibble(
    indicator = code,
    geo       = as.character(geo),
    year      = as.integer(year),
    value     = suppressWarnings(as.numeric(val)),
    unit      = if ("unit" %in% names(df)) as.character(df$unit) else NA_character_,
    nace_r2   = if ("nace_r2" %in% names(df)) as.character(df$nace_r2) else NA_character_,
    indic_sb  = if ("indic_sb" %in% names(df)) as.character(df$indic_sb) else NA_character_,
    s_adj     = if ("s_adj" %in% names(df)) as.character(df$s_adj) else NA_character_,
    geo_label = if ("geo_label" %in% names(df)) as.character(df$geo_label) else NA_character_
  ) %>% filter(!is.na(geo))  # do NOT drop on year
}

get_tbl <- function(tbl_code, dump_csv = TRUE){
  suppressMessages({
    dat <- eurostat::get_eurostat(
      tbl_code, time_format = "date", cache = TRUE, type = "code", keepFlags = FALSE
    )
  })
  dat_labeled <- tryCatch(eurostat::label_eurostat(dat, dic = "geo", lang = "en"),
                          error = function(e) dat)
  
  # Save raw RDS + CSV
  readr::write_rds(dat_labeled, file.path("data_raw", paste0("eurostat_", tbl_code, ".rds")), compress = "gz")
  if (dump_csv) readr::write_csv(dat_labeled, file.path("data_raw", paste0("eurostat_", tbl_code, ".csv")))
  
  yrs  <- tryCatch(lubridate::year(dat_labeled$time), error = function(e) NA_integer_)
  geos <- length(unique(na.omit(if ("geo" %in% names(dat_labeled)) dat_labeled$geo else if ("geo_code" %in% names(dat_labeled)) dat_labeled$geo_code else NA)))
  message(sprintf("  - %s: %s rows | years [%s…%s] | %s geos",
                  tbl_code, format(nrow(dat_labeled), big.mark=","), safe_min(yrs), safe_max(yrs), geos))
  
  dat_labeled
}

# ---------- Download ----------
raw_list <- purrr::map(tables, ~{ message("Table ", .x); get_tbl(.x, dump_csv = TRUE) })
names(raw_list) <- tables

# ---------- Normalise ----------
norm <- purrr::imap_dfr(raw_list, ~norm_one(.x, .y))

# ---------- Domain filters ----------
ht_nace <- c("J61","J62","J63","C26","M72")
norm <- norm %>% mutate(nace_r2 = ifelse(is.na(nace_r2), "", nace_r2))

norm_sbs <- norm %>% filter(indicator == tbl_sbs) %>%
  filter(str_detect(nace_r2, paste0("^(", paste(ht_nace, collapse="|"), ")")))
norm_other <- norm %>% filter(indicator != tbl_sbs)
norm <- dplyr::bind_rows(norm_other, norm_sbs)

# Relabel indicators + region label
norm <- norm %>%
  mutate(indicator = dplyr::case_when(
    indicator == tbl_gerd     ~ "GERD_regional",
    indicator == tbl_htec_emp ~ "HighTech_Employment_regional",
    indicator == tbl_patents  ~ "HighTech_patents_regional",
    indicator == tbl_sbs      ~ "SBS_hightech_regional",
    indicator == tbl_highgrow ~ "HighGrowth_enterprises_share_regional",
    indicator == tbl_vc_nat   ~ "VentureCapital_pct_GDP_national",
    TRUE ~ indicator
  )) %>%
  mutate(region = dplyr::coalesce(geo_label, geo)) %>%
  select(indicator, geo, year, value, unit, nace_r2, indic_sb, s_adj, region)

# ---------- City extraction (with national fallback) ----------
pat_for   <- function(cty) targets$nuts_pattern[targets$city == cty]
label_for <- function(cty) targets$region_label_canon[targets$city == cty]

is_country_code <- function(x) {
  x <- na.omit(x)
  # Country codes are usually 2 letters; exclude composites like EU27_2020
  nchar(x) <= 2 & grepl("^[A-Z]{2}$", x)
}

pick_cities <- function(df, target_tbl){
  # helper for national series
  handle_country_level <- function(d, who){
    keep_cc <- c("EE","ES","EL","PT")
    d <- d %>% filter(geo %in% keep_cc)
    if (nrow(d) > 0) {
      message(sprintf("  ℹ Using NATIONAL fallback for %s (%s rows for EE/ES/EL/PT).", who, nrow(d)))
    }
    d %>%
      mutate(city = dplyr::case_when(
        geo == "EE" ~ "Tallinn",
        geo == "ES" ~ "Barcelona",
        geo == "EL" ~ "Athens",
        geo == "PT" ~ "Lisbon",
        TRUE ~ NA_character_
      )) %>%
      mutate(region = geo) %>%
      select(indicator, geo, year, value, unit, region, city)
  }
  
  # Venture capital is national
  if (target_tbl == "VentureCapital_pct_GDP_national") {
    return(handle_country_level(df %>% filter(indicator == target_tbl), target_tbl))
  }
  
  # If SBS/HighGrowth are effectively national (most geos 2-letter & our 4 present), use national path
  if (target_tbl %in% c("SBS_hightech_regional", "HighGrowth_enterprises_share_regional")) {
    d_sub <- df %>% filter(indicator == target_tbl)
    if (nrow(d_sub) > 0) {
      share_two_letter <- mean(is_country_code(d_sub$geo))
      has_our_countries <- any(d_sub$geo %in% c("EE","ES","EL","PT"))
      if (isTRUE(share_two_letter > 0.5) && has_our_countries) {
        return(handle_country_level(d_sub, target_tbl))
      }
    }
  }
  
  # Default: regional (NUTS2/3)
  reg <- df %>%
    filter(indicator == target_tbl) %>%
    mutate(city = dplyr::case_when(
      str_detect(geo, pat_for("Tallinn"))   ~ "Tallinn",
      str_detect(geo, pat_for("Barcelona")) ~ "Barcelona",
      str_detect(geo, pat_for("Athens"))    ~ "Athens",
      str_detect(geo, pat_for("Lisbon"))    ~ "Lisbon",
      TRUE ~ NA_character_
    )) %>%
    filter(!is.na(city)) %>%
    mutate(region = dplyr::case_when(
      city=="Tallinn"   ~ label_for("Tallinn"),
      city=="Barcelona" ~ label_for("Barcelona"),
      city=="Athens"    ~ label_for("Athens"),
      city=="Lisbon"    ~ label_for("Lisbon"),
      TRUE ~ region
    )) %>%
    select(indicator, geo, year, value, unit, nace_r2, indic_sb, s_adj, region, city)
  
  if (nrow(reg) == 0) {
    message(sprintf("  ⚠ %s had 0 rows after city selection — likely NUTS level mismatch in this table.", target_tbl))
  }
  reg
}

pieces <- dplyr::bind_rows(
  pick_cities(norm, "GERD_regional"),
  pick_cities(norm, "HighTech_Employment_regional"),
  pick_cities(norm, "HighTech_patents_regional"),
  pick_cities(norm, "SBS_hightech_regional"),
  pick_cities(norm, "HighGrowth_enterprises_share_regional"),
  pick_cities(norm, "VentureCapital_pct_GDP_national")
)

# ---------- Final tidy ----------
final <- pieces %>%
  arrange(city, indicator, year) %>%
  mutate(
    geo_level = case_when(
      str_detect(geo, "^[A-Z]{2}\\d{2,3}$") ~ "NUTS2/3",
      str_detect(geo, "^[A-Z]{2}$")         ~ "Country",
      TRUE ~ "Unknown"
    )
  )

# ---------- Write per city ----------
write_city <- function(cty){
  out <- final %>% filter(city == cty)
  if (nrow(out) == 0) {
    warning(sprintf("No rows for %s — check NUTS patterns and table coverage.", cty))
    return(invisible(NULL))
  }
  csv_fp  <- file.path("data_clean", paste0(gsub(" ", "_", tolower(cty)), "_indicators.csv"))
  xlsx_fp <- file.path("data_clean", paste0(gsub(" ", "_", tolower(cty)), "_indicators.xlsx"))
  readr::write_csv(out, csv_fp)
  sheets <- out %>% group_split(indicator, .keep = TRUE)
  names(sheets) <- out %>% distinct(indicator) %>% pull(indicator)
  writexl::write_xlsx(lapply(sheets, as.data.frame), xlsx_fp)
  message(" - wrote ", csv_fp, " (", nrow(out), " rows)")
  message(" - wrote ", xlsx_fp)
}

message("Writing per-city outputs to data_clean/ ...")
c("Tallinn","Barcelona","Athens","Lisbon") %>% purrr::walk(write_city)

# ---------- Summary ----------
if (nrow(final) == 0) {
  message("❗ No rows made it to final. Check NUTS patterns or table availability for these geographies.")
} else {
  message("Summary by indicator and city:")
  final %>%
    count(indicator, city, name = "rows") %>%
    arrange(indicator, city) %>%
    print(n = 100)
}

message("✅ Eurostat pull complete.")
