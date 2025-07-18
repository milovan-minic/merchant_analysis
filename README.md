# Merchant Analysis Project

This project provides a robust, scalable pipeline for merchant transaction analysis using PySpark, with all steps orchestrated for Dockerized environments. The workflow includes data cleaning, deduplication, and a suite of business analyses, producing actionable reports and data exports.

---

## Project Structure

```
merchant_analysis/
├── app/
│   ├── __init__.py
│   ├── cleaning.py
│   ├── download_data.py
│   ├── preprocess_data.py
│   ├── run_all.py
│   ├── utils.py
│   ├── README.md
│   ├── notebooks/
│   │   └── eda_basic.ipynb
│   └── analysis/
│       ├── __init__.py
│       ├── q1_top_merchants.py
│       ├── q2_avg_sale.py
│       ├── q3_top_hours.py
│       ├── q4_popular_merchants.py
│       └── q5_advice.py
├── data/
│   ├── in/
│   │   ├── .gitkeep
│   │   ├── data_dictionary.xlsx
│   │   ├── merchants-subset.csv
│   │   └── historical_transactions.parquet
│   ├── clean/
│   │   ├── .gitkeep
│   │   ├── merchants_cleaned.parquet/
│   │   └── historical_transactions_cleaned.parquet/
│   └── out/
│       └── .gitkeep
├── reports/
│   ├── .gitkeep
│   ├── q1_top_merchants.csv/
│   ├── q2_avg_sale.csv/
│   ├── q3_top_hours.csv/
│   ├── q4_popular_merchants.csv/
│   ├── q4_city_category_crosstab.csv/
│   ├── q5_top_cities.csv/
│   ├── q5_top_categories.csv/
│   ├── q5_interesting_months.csv/
│   ├── q5_top_hours.csv/
│   └── q5_advice.md
├── .gitignore
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```

---

## Environment Setup

### Prerequisites

- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
- (Optional) Python 3.8+ if running locally without Docker

### Quick Start (Recommended: Docker Compose)

1. **Clone the repository:**
   ```bash
   git clone <repo-url>
   cd merchant_analysis
   ```

2. **Download the data:**
   - Place the following files in `data/in/`:
     - `merchants-subset.csv`
     - `historical_transactions.parquet`
     - `data_dictionary.xlsx`
   - (If not present, use the provided `app/download_data.py` or follow project instructions.)

3. **Build and start the Spark container:**
   ```bash
   docker compose up
   ```

4. **Run the full pipeline:**
   ```bash
   docker compose exec spark python app/run_all.py
   ```
   This will:
   - Clean and deduplicate all raw data (one-time ETL step)
   - Run all analysis scripts in sequence
   - Generate all reports in the `reports/` directory

---

## Data Workflow

### 1. **Preprocessing (ETL)**
- Script: `app/preprocess_data.py`
- Loads raw data from `data/in/`
- Cleans (trims, standardizes, fills missing, deduplicates)
- Saves cleaned data to `data/clean/` as Parquet files:
  - `merchants_cleaned.parquet`
  - `historical_transactions_cleaned.parquet`

### 2. **Analysis**
- Scripts in `app/analysis/` use only the cleaned data.
- Each script produces a CSV (in a subfolder) in `reports/` and prints a summary.

### 3. **Business Advice Report**
- Script: `app/analysis/q5_advice.py`
- Produces a Markdown report: `reports/q5_advice.md`
- Summarizes key findings and recommendations for new merchants.

---

## Output Files

- **Cleaned Data:**  
  - `data/clean/merchants_cleaned.parquet`
  - `data/clean/historical_transactions_cleaned.parquet`

- **Analysis Results:**  
  - CSVs in `reports/` (e.g., `q1_top_merchants.csv/part-*.csv`)
  - Markdown report: `reports/q5_advice.md`

- **CSV Output Note:**  
  Each CSV output is in a subdirectory (e.g., `reports/q1_top_merchants.csv/`) and may be partitioned (look for `part-*.csv`).

---

## How to Run Analyses

- **Full pipeline:**  
  ```bash
  python app/run_all.py
  ```
  (Or use the Docker Compose command above.)

- **Individual analysis:**  
  ```bash
  python -m app.analysis.q1_top_merchants
  ```

---

## Understanding the Outputs

- **CSV files:**  
  Contain tabular results for each analysis question (top merchants, average sales, top hours, etc.).
- **Markdown report (`q5_advice.md`):**  
  Provides business recommendations, including:
  - Top cities and categories by sales
  - Seasonal/monthly trends
  - Most profitable hours
  - Installment profitability analysis

---

## Customization & Extensibility

- Add new analysis scripts to `app/analysis/` and include them in `run_all.py`.
- Adjust cleaning logic in `app/preprocess_data.py` as needed for new data sources.
- The pipeline is modular and ready for scaling or integration with additional Spark services.

---

## Troubleshooting

- **Performance:**  
  All cleaning and deduplication is performed once during preprocessing for maximum efficiency.
- **Data issues:**  
  Check the `data/in/` and `data/clean/` folders for input/output consistency.
- **Logs:**  
  Spark logs are output to the console; adjust log level in scripts as needed.

---

## Contact

I'd love to hear from you! For questions, suggestions, or collaboration, please reach out or create a pull request:

- **Name:** Milovan Minic
- **Email:** [milovan.minic@gmail.com](mailto:milovan.minic@gmail.com?subject=Merchant%20Analysis%20github%20repository%20inquiry)
- **LinkedIn:** [https://www.linkedin.com/in/milovan-minic](https://www.linkedin.com/in/milovan-minic)
- **GitHub:** [https://github.com/milovan-minic](https://github.com/milovan-minic)

Feel free to connect or open an issue/pull request if you have ideas to improve this project! 