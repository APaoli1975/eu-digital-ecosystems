# EU Digital Ecosystems — Executive MBA Thesis

**Author:** Andrea Paoli  
**Programme:** MBA Leadership — Negotiated Learning Project  
**Institution:** International Business School - University of Lincoln
**Date:** August 2025

---

## 📖 Overview

This repository contains all files, datasets, scripts, and outputs related to my Executive MBA final thesis:  
**"EU Digital Ecosystems: Data-Driven Insights for Policy and Strategy"**.

The project aims to explore the structure, performance, and interconnections of digital ecosystems across the European Union, with case studies focusing on Tallinn and Barcelona. The analysis combines economic, technological, and policy perspectives, using open statistical data and reproducible methods in R and Quarto.

---

# eu-digital-ecosystems

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![R](https://img.shields.io/badge/R-4.5.1-blue.svg)
![Quarto](https://img.shields.io/badge/Quarto-1.7.33-lightgrey.svg)
![Last Commit](https://img.shields.io/github/last-commit/APaoli1975/eu-digital-ecosystems?color=green)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.16786258.svg)](https://doi.org/10.5281/zenodo.16786258)


---

## 🗂 Repository Structure

```
eu-digital-ecosystems/
├── data_raw/             # Original raw datasets (not modified)
├── data_clean/           # Processed/cleaned datasets ready for analysis
├── figures/              # Generated figures and charts (ignored in Git)
├── outputs/              # Tables, reports, and other generated outputs (ignored in Git)
├── scripts/              # R scripts for analysis and data preparation
├── docs/                 # Draft chapters, notes, and Word exports
├── analysis-and-outputs.qmd  # Quarto notebook for analysis
├── .gitignore
├── .gitattributes
├── LICENSE
└── README.md
```


---

## 🔍 Data Sources

Data will be gathered from:
- **Eurostat** — European statistical office datasets
- **City Open Data Portals** (Tallinn, Barcelona)
- **OECD Data** — Economic and innovation indicators
- **European Commission** — Digital Economy and Society Index (DESI)

---

## 📊 Tools & Technologies

The analysis uses an **R + Quarto** workflow:
- **R Packages:** `tidyverse`, `janitor`, `broom`, `modelsummary`, `gt`, `sf`, `tmap`, `eurostat`, `plotly`, `DT`
- **Quarto** for rendering analysis outputs
- **Git & GitHub** for version control and backup

---

## 📈 Project Objectives

1. Design a reproducible, transparent workflow for data collection and analysis.
2. Evaluate digital ecosystem maturity in two EU cities (Tallinn and Barcelona).
3. Identify actionable insights for EU-level digital policy and local governance.

---

## 📌 Notes

- Figures and outputs are not tracked in Git to keep the repository light.
- All code and analysis are reproducible; anyone can clone the repository and run the `.qmd` file to regenerate results.

---

## ⚖️ License

This repository is licensed under the [MIT License](LICENSE) unless otherwise stated.
