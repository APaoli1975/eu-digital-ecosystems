# scripts/01_pull_eurostat.R
# Build two tidy files: data_clean/tallinn_indicators.csv and data_clean/barcelona_indicators.csv

req <- c("eurostat","dplyr","readr","janitor","stringr","purrr","rlang")
missing <- setdiff(req, rownames(installed.packages()))
if (length(missing)) install.packages(missing, dependencies = TRUE)

library(eurostat)
library(dplyr)
library(readr)
library(janitor)
library(stringr)
library(purrr)
library(rlang)

dir.create("data_raw",  showWarnings = FALSE)
dir.create("data_clean", showWarnings = FALSE)

# Targets
nuts3_tallinn   <- "EE001"  # Harju (Tallinn)
nuts3_barcelona <- "ES511"  # Barcelonès (Barcelona)
vc_countries    <- c("EE","ES")

# Helper: standardise common columns
standardise_cols <- function(df) {
  df <- clean_names(df)
  if ("time_period" %in% names(df) && !"time" %in% names(df)) df <- rename(df, time = time_period)
  if ("year" %in% names(df)        && !"time" %in% names(df)) df <- rename(df, time = year)
  if ("value" %in% names(df)       && !"values" %in% names(df)) df <- rename(df, values = value)
  df
}

save_raw <- function(df, stem) {
  write_csv(df, file.path("data_raw", paste0(stem, ".csv")))
  saveRDS(df,              file.path("data_raw", paste0(stem, ".rds")))
}

add_tag <- function(df, tag) df %>% mutate(indicator = tag)

# Pull datasets
df_gerd <- get_eurostat("rd_e_gerdreg", time_format = "num") %>% standardise_cols()
save_raw(df_gerd, "eurostat_rd_e_gerdreg_full")

df_htec <- get_eurostat("htec_emp_reg2", time_format = "num") %>% standardise_cols()
save_raw(df_htec, "eurostat_htec_emp_reg2_full")

df_pat  <- get_eurostat("pat_ep_rtec",   time_format = "num") %>% standardise_cols()
save_raw(df_pat,  "eurostat_pat_ep_rtec_full")

df_sbs  <- get_eurostat("sbs_na_ind_r2", time_format = "num") %>% standardise_cols()
save_raw(df_sbs,  "eurostat_sbs_na_ind_r2_full")

df_hg   <- get_eurostat("bd_9pm_r2",     time_format = "num") %>% standardise_cols()
save_raw(df_hg,   "eurostat_bd_9pm_r2_full")

df_vc   <- get_eurostat("tec00040",      time_format = "num") %>% standardise_cols()
save_raw(df_vc,   "eurostat_tec00040_full")

# Filter SBS to core digital/deep-tech sectors (your chosen set)
sbs_sectors_regex <- "^(J61|J62|J63|C26|M72)"
df_sbs_filt <- df_sbs %>% filter(str_detect(nace_r2 %||% "", sbs_sectors_regex))

# Build a single long table for each geo
make_region_table <- function(region_code, region_label, country_code_for_vc) {
  
  gerd <- df_gerd %>%
    filter(geo == region_code) %>%
    add_tag("GERD_RnD_expenditure")
  
  htec <- df_htec %>%
    filter(geo == region_code) %>%
    add_tag("HighTech_employment")
  
  pat  <- df_pat %>%
    filter(geo == region_code) %>%
    add_tag("HighTech_patents")
  
  sbs  <- df_sbs_filt %>%
    filter(geo == region_code) %>%
    add_tag("SBS_ICT_M72_metrics")
  
  hg   <- df_hg %>%
    filter(geo == region_code | geo == country_code_for_vc) %>%  # if NUTS3 not available, keep country rows for context
    add_tag("HighGrowth_enterprises_share")
  
  vc   <- df_vc %>%
    filter(geo == country_code_for_vc) %>%
    mutate(geo_level = "country") %>%
    add_tag("VentureCapital_pct_GDP")
  
  out <- bind_rows(gerd, htec, pat, sbs, hg, vc) %>%
    # keep common identifiers; tolerate missing dims
    select(
      indicator, geo, time, values,
      unit      = any_of("unit"),
      nace_r2   = any_of("nace_r2"),
      indic_sb  = any_of("indic_sb"),
      s_adj     = any_of("s_adj"),
      wih2      = any_of("wih2"),
      patent    = any_of("patent"),
      techno    = any_of("techno"),
      geo_level = any_of("geo_level")
    ) %>%
    arrange(indicator, time) %>%
    mutate(region = region_label)
  
  out
}

# Build per-region datasets
tallinn_tbl   <- make_region_table(nuts3_tallinn,   "Tallinn (EE001)",   "EE")
barcelona_tbl <- make_region_table(nuts3_barcelona, "Barcelona (ES511)", "ES")

# Write outputs
write_csv(tallinn_tbl,   "data_clean/tallinn_indicators.csv")
write_csv(barcelona_tbl, "data_clean/barcelona_indicators.csv")

message("✅ Done. Wrote:")
message(" - data_clean/tallinn_indicators.csv")
message(" - data_clean/barcelona_indicators.csv")
