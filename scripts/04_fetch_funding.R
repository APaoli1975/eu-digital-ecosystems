# scripts/04_fetch_funding.R
# Purpose: Build a city-year EU funding panel for 4 case studies from a local Cohesion CSV.
# - Reads data_raw/EU_funding.csv (Historic EU payments – regionalised & modelled)
# - Uses direct NUTS2 when available (EL30, PT17) and *proxy* NUTS where needed:
#     Barcelona → ES511 (preferred), else ES51 (Catalonia, NUTS2)   [proxy]
#     Tallinn   → EE001 (preferred), else EE00 (Estonia, NUTS2)     [proxy]
# - Aggregates across funds per year; tries to compute per-capita via Eurostat NUTS2 population
# - Emits: data_clean/eu_funding_panel.csv with
#     city, year, eu_funding, funds_pc, funding_geo, funding_method

suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(tidyr); library(janitor)
  library(eurostat)
})

# persistent cache for eurostat to avoid temp-path hiccups on Windows
euro_cache <- file.path("data_raw", "eurostat_cache")
if (!dir.exists(euro_cache)) dir.create(euro_cache, recursive = TRUE)
options(eurostat_cache_dir = euro_cache)

msg <- function(...) cat("[04-funding] ", sprintf(...), "\n")

# ---------- INPUT ----------
in_csv <- "data_raw/EU_funding.csv"
if (!file.exists(in_csv)) stop("[04-funding] ❌ Missing file: ", in_csv, call. = FALSE)

# ---------- READ & NORMALISE ----------
raw <- readr::read_csv(in_csv, show_col_types = FALSE)
msg("Rows read: %s", nrow(raw))
df  <- raw |> janitor::clean_names()

need <- c("nuts2_id","nuts2_name","year","eu_payment_annual")
if (any(!need %in% names(df))) {
  stop("[04-funding] ❌ Needed columns not found: ",
       paste(setdiff(need, names(df)), collapse = ", "),
       "\nHad: ", paste(names(df), collapse = ", "), call. = FALSE)
}

to_num <- function(x) as.numeric(gsub(",", "", x))
df <- df %>%
  mutate(
    year = suppressWarnings(as.integer(year)),
    eu_payment_annual = to_num(eu_payment_annual)
  ) %>%
  filter(!is.na(year), !is.na(nuts2_id))

# ---------- CITY → GEO (direct/proxy) ----------
geo_pref <- tibble::tibble(
  city          = c("Athens","Lisbon","Barcelona","Tallinn"),
  primary_code  = c("EL30",  "PT17",   "ES511",    "EE001"),
  fallback_code = c(NA,      NA,       "ES51",     "EE00")  # Catalonia, Estonia (NUTS2)
)

choose_geo <- function(primary_code, fallback_code, df_codes) {
  has_primary  <- primary_code %in% df_codes
  has_fallback <- !is.na(fallback_code) && fallback_code %in% df_codes
  if (has_primary)  return(list(code = primary_code,  method = "direct"))
  if (has_fallback) return(list(code = fallback_code, method = "proxy"))
  return(list(code = NA_character_, method = "missing"))
}

codes_in_file <- unique(df$nuts2_id)
geo_used <- geo_pref %>%
  rowwise() %>%
  mutate(sel = list(choose_geo(primary_code, fallback_code, codes_in_file))) %>%
  tidyr::unnest_wider(sel) %>%
  ungroup() %>%
  rename(funding_geo = code, funding_method = method)

msg("Geographies selected per city:")
print(geo_used, n = 10)

# ---------- BUILD CITY SERIES ----------
build_city_series <- function(city, code) {
  if (is.na(code)) return(tibble(city = character(), year = integer(), eu_funding = numeric()))
  df %>%
    filter(nuts2_id == code) %>%
    group_by(year) %>%
    summarise(eu_funding = sum(eu_payment_annual, na.rm = TRUE), .groups = "drop") %>%
    mutate(city = city)
}

fund_city <- bind_rows(
  build_city_series("Athens",    geo_used$funding_geo[geo_used$city=="Athens"]),
  build_city_series("Lisbon",    geo_used$funding_geo[geo_used$city=="Lisbon"]),
  build_city_series("Barcelona", geo_used$funding_geo[geo_used$city=="Barcelona"]),
  build_city_series("Tallinn",   geo_used$funding_geo[geo_used$city=="Tallinn"])
) %>%
  left_join(select(geo_used, city, funding_geo, funding_method), by = "city") %>%
  arrange(city, year)

msg("Coverage by city (years with any funding):")
print(fund_city %>% filter(eu_funding > 0 | !is.na(eu_funding)) %>% count(city), n = 10)

# ---------- EUROSTAT POPULATION (robust multi-dataset fallback) ----------
get_pop_try <- function(geos_needed){
  # Attempt 1: demo_r_pjangr3 (NUTS-2, total by summing ages)
  pop <- try({
    eurostat::get_eurostat("demo_r_pjangr3", time_format = "num", cache = TRUE, keepFlags = FALSE) %>%
      filter(geo %in% geos_needed, sex == "T") %>%
      group_by(geo, time) %>% summarise(pop = sum(values, na.rm = TRUE), .groups = "drop") %>%
      rename(funding_geo = geo, year = time)
  }, silent = TRUE)
  if (inherits(pop, "try-error")) pop <- NULL
  if (!is.null(pop) && nrow(pop)) return(pop)
  
  # Attempt 2: demo_r_pjanaggr3 (NUTS-2, age groups) → age = "TOTAL"
  pop <- try({
    eurostat::get_eurostat("demo_r_pjanaggr3", time_format = "num", cache = TRUE, keepFlags = FALSE) %>%
      filter(geo %in% geos_needed, sex == "T", age == "TOTAL") %>%
      select(geo, time, values) %>%
      rename(funding_geo = geo, year = time, pop = values)
  }, silent = TRUE)
  if (inherits(pop, "try-error")) pop <- NULL
  if (!is.null(pop) && nrow(pop)) return(pop)
  
  # Attempt 3: demo_r_pjanind (indicator table) → indic_de == "JAN" (January population)
  pop <- try({
    eurostat::get_eurostat("demo_r_pjanind", time_format = "num", cache = TRUE, keepFlags = FALSE) %>%
      filter(geo %in% geos_needed, indic_de == "JAN") %>%
      select(geo, time, values) %>%
      rename(funding_geo = geo, year = time, pop = values)
  }, silent = TRUE)
  if (inherits(pop, "try-error")) pop <- NULL
  if (!is.null(pop) && nrow(pop)) return(pop)
  
  NULL
}

geos_needed <- sort(unique(na.omit(geo_used$funding_geo)))
msg("Downloading Eurostat population for %s ...", paste(geos_needed, collapse = ", "))

pop_sel <- get_pop_try(geos_needed)
if (is.null(pop_sel) || !nrow(pop_sel)) {
  msg("⚠ Population download failed across all fallbacks. Proceeding with funds_pc = NA.")
  fund_city <- fund_city %>%
    mutate(funds_pc = NA_real_) %>%
    select(city, year, eu_funding, funds_pc, funding_geo, funding_method) %>%
    arrange(city, year)
} else {
  fund_city <- fund_city %>%
    left_join(pop_sel, by = c("funding_geo","year")) %>%
    mutate(funds_pc = ifelse(!is.na(pop) & pop > 0, eu_funding / pop, NA_real_)) %>%
    select(city, year, eu_funding, funds_pc, funding_geo, funding_method) %>%
    arrange(city, year)
}

# ---------- WRITE ----------
dir.create("data_clean", showWarnings = FALSE)
out <- "data_clean/eu_funding_panel.csv"
readr::write_csv(fund_city, out)
msg("✅ Wrote %s (%d rows).", out, nrow(fund_city))


