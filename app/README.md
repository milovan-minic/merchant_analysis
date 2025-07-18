# PySpark Application Directory

This directory contains all the core application code for the Merchant Analysis Project. The code is modular, maintainable, and designed for scalable data processing using PySpark.

---

## Directory Structure

```
app/
├── __init__.py
├── cleaning.py           # General-purpose data cleaning utilities
├── download_data.py      # Script to download raw data files
├── preprocess_data.py    # ETL: Cleans and deduplicates raw data for analysis
├── run_all.py            # Orchestrates the full pipeline (preprocessing + analysis)
├── utils.py              # Utility functions for loading and transforming data
├── README.md             # This documentation file
├── notebooks/
│   └── eda.ipynb         # Exploratory Data Analysis (Jupyter notebook)
└── analysis/
    ├── __init__.py
    ├── q1_top_merchants.py
    ├── q2_avg_sale.py
    ├── q3_top_hours.py
    ├── q4_popular_merchants.py
    └── q5_advice.py
```

---

## Main Components

### 1. **Data Cleaning & Utilities**
- **`cleaning.py`**:  
  Contains reusable functions for trimming, standardizing, deduplicating, and handling missing values in Spark DataFrames.  
  Centralizes all cleaning logic for consistency and performance.

- **`utils.py`**:  
  Functions for loading data (CSV, Parquet), adding derived columns, and other common transformations.

### 2. **ETL & Orchestration**
- **`preprocess_data.py`**:  
  Loads raw data from `data/in/`, applies all cleaning and deduplication, and saves the cleaned data to `data/clean/`.  
  This step is run once per data update, ensuring all downstream analysis uses consistent, high-quality data.

- **`run_all.py`**:  
  Runs the full pipeline: preprocessing, then all analysis scripts in sequence.  
  Designed for use in Docker or local environments.

### 3. **Analysis Scripts**
- **`analysis/`**:  
  Contains one script per business question or report.  
  Each script loads only the cleaned data, performs its analysis, and writes results to the `reports/` directory.

### 4. **Data Download & EDA**
- **`download_data.py`**:  
  Script to automate downloading of raw data files from remote sources.

- **`notebooks/eda.ipynb`**:  
  Jupyter notebook for exploratory data analysis, data profiling, and prototyping.

---

## Design Considerations

- **Separation of Concerns**:  
  Cleaning, ETL, and analysis are strictly separated for maintainability and performance.

- **Reusability**:  
  All cleaning and utility functions are centralized for easy reuse and testing.

- **Performance**:  
  Cleaning and deduplication are performed once during preprocessing, minimizing repeated computation.

- **Extensibility**:  
  New analysis scripts can be added to `analysis/` and included in `run_all.py` with minimal changes.

- **Docker-Ready**:  
  All scripts are compatible with containerized Spark environments.

---

## How to Extend

- Add new cleaning logic to `cleaning.py` and update `preprocess_data.py` as needed.
- Add new analysis scripts to `analysis/` and update `run_all.py` to include them.
- Use `notebooks/` for prototyping and EDA before productionizing code.

---

## Usage

- **Run the full pipeline:**  
  ```bash
  python run_all.py
  ```
- **Run a specific analysis:**  
  ```bash
  python -m analysis.q1_top_merchants
  ```

---

For more details on the overall project, see the root `README.md`.