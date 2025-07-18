import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pyspark.sql import SparkSession, functions as F, Window
from app.utils import load_transactions, fill_missing_category, add_month_column, add_hour_column

DATA_DIR = '/home/jovyan/data/clean'
REPORTS_DIR = '/home/jovyan/reports'
MERCHANTS_PATH = os.path.join(DATA_DIR, 'merchants_cleaned.parquet')
TRANSACTIONS_PATH = os.path.join(DATA_DIR, 'historical_transactions_cleaned.parquet')
OUTPUT_PATH = os.path.join(REPORTS_DIR, 'q5_advice.md')

def get_top_cities(transactions, csv_path):
    city_sales = transactions.groupBy('city_id').agg(
        F.sum('purchase_amount').alias('total_sales'),
        F.count('*').alias('num_sales')
    ).orderBy(F.desc('total_sales'))
    top_cities_df = city_sales.limit(5)
    # Save to CSV
    top_cities_df.coalesce(1).write.mode('overwrite').option('header', True).csv(csv_path)
    top_cities = top_cities_df.toPandas()
    result = f"### a. Recommended Cities\n\nData: `{csv_path}`\n\n"
    result += top_cities.to_markdown(index=False)
    result += "\n\nRecommendation: Focus on cities with the highest total sales."
    return result


def get_top_categories(transactions, csv_path):
    transactions = fill_missing_category(transactions)
    cat_sales = transactions.groupBy('category').agg(
        F.sum('purchase_amount').alias('total_sales'),
        F.count('*').alias('num_sales')
    ).orderBy(F.desc('total_sales'))
    top_cats_df = cat_sales.limit(5)
    # Save to CSV
    top_cats_df.coalesce(1).write.mode('overwrite').option('header', True).csv(csv_path)
    top_cats = top_cats_df.toPandas()
    result = f"### b. Recommended Categories\n\nData: `{csv_path}`\n\n"
    result += top_cats.to_markdown(index=False)
    result += "\n\nRecommendation: Focus on categories with the highest total sales."
    return result


def get_interesting_months(transactions, csv_path):
    transactions = add_month_column(transactions)
    month_sales = transactions.groupBy('month').agg(
        F.sum('purchase_amount').alias('total_sales'),
        F.count('*').alias('num_sales')
    ).orderBy('month')
    # Save to CSV
    month_sales.coalesce(1).write.mode('overwrite').option('header', True).csv(csv_path)
    months = month_sales.toPandas()
    result = f"### c. Interesting Months\n\nData: `{csv_path}`\n\n"
    result += months.to_markdown(index=False)
    result += "\n\nRecommendation: Look for months with sales spikes or seasonality."
    return result


def get_top_hours(transactions, csv_path):
    transactions = add_hour_column(transactions)
    hour_sales = transactions.groupBy('hour').agg(
        F.sum('purchase_amount').alias('total_sales'),
        F.count('*').alias('num_sales')
    ).orderBy(F.desc('total_sales'))
    top_hours_df = hour_sales.limit(5)
    # Save to CSV
    top_hours_df.coalesce(1).write.mode('overwrite').option('header', True).csv(csv_path)
    top_hours = top_hours_df.toPandas()
    result = f"### d. Recommended Hours\n\nData: `{csv_path}`\n\n"
    result += top_hours.to_markdown(index=False)
    result += "\n\nRecommendation: Open during hours with the highest sales."
    return result

def analyze_installments(transactions):
    # Assume 'installments' column exists
    # 25% gross profit, 22.9% default rate per month, default after half payment
    total_sales = transactions.agg(F.sum('purchase_amount')).first()[0]
    with_installments = transactions.filter(F.col('installments') > 1)
    total_installment_sales = with_installments.agg(F.sum('purchase_amount')).first()[0] or 0
    gross_profit = total_installment_sales * 0.25
    expected_loss = total_installment_sales * 0.5 * 0.229
    net_profit = gross_profit - expected_loss
    result = f"### e. Installments Analysis\n"
    result += f"Total sales with installments: {total_installment_sales:,.2f}\n\n"
    result += f"Gross profit (25%): {gross_profit:,.2f}\n\n"
    result += f"Expected loss due to default (22.9% after half payment): {expected_loss:,.2f}\n\n"
    result += f"Net profit from installment sales: {net_profit:,.2f}\n\n"
    if net_profit > 0:
        result += "Recommendation: Accepting installments is profitable under these assumptions."
    else:
        result += "Recommendation: Accepting installments may not be profitable under these assumptions."
    return result

def main():
    spark = SparkSession.builder.getOrCreate()

    # Set log level to ERROR to reduce log noise
    spark.sparkContext.setLogLevel('ERROR')

    transactions = spark.read.parquet(TRANSACTIONS_PATH)
    
    # Define output CSV paths
    top_cities_csv = os.path.join(REPORTS_DIR, 'q5_top_cities.csv')
    top_categories_csv = os.path.join(REPORTS_DIR, 'q5_top_categories.csv')
    months_csv = os.path.join(REPORTS_DIR, 'q5_interesting_months.csv')
    top_hours_csv = os.path.join(REPORTS_DIR, 'q5_top_hours.csv')
    report = "# Question 5: Business Advice for New Merchant\n\n"
    report += get_top_cities(transactions, top_cities_csv) + "\n\n"
    report += get_top_categories(transactions, top_categories_csv) + "\n\n"
    report += get_interesting_months(transactions, months_csv) + "\n\n"
    report += get_top_hours(transactions, top_hours_csv) + "\n\n"
    report += analyze_installments(transactions) + "\n"
    with open(OUTPUT_PATH, 'w') as f:
        f.write(report)
    print(f"Advice report saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
