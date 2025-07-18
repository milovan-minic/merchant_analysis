import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pyspark.sql import SparkSession, Window
from app.utils import load_transactions, fill_missing_category, add_hour_column
from pyspark.sql import functions as F

# Paths
DATA_DIR = '/home/jovyan/data/clean'
REPORTS_DIR = '/home/jovyan/reports'
TRANSACTIONS_PATH = os.path.join(DATA_DIR, 'historical_transactions_cleaned.parquet')
OUTPUT_PATH = os.path.join(REPORTS_DIR, 'q3_top_hours.csv')

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Set log level to ERROR to reduce log noise
spark.sparkContext.setLogLevel('ERROR')

# Load cleaned data
transactions = spark.read.parquet(TRANSACTIONS_PATH)

# Add hour column
transactions = add_hour_column(transactions)

# Aggregate purchase_amount by category and hour
agg = transactions.groupBy('category', 'hour').agg(
    F.sum('purchase_amount').alias('Total Purchase Amount')
)

# Rank hours within each category by total purchase amount
window = Window.partitionBy('category').orderBy(F.desc('Total Purchase Amount'))
agg = agg.withColumn('rank', F.row_number().over(window))

# Filter top 3 hours per category
top3 = agg.filter(F.col('rank') <= 3)

# Select and order columns as required
result = top3.select(
    'category',
    'hour',
    'Total Purchase Amount'
).orderBy('category', F.desc('Total Purchase Amount'))

# Save to CSV
result.coalesce(1).write.csv(OUTPUT_PATH, header=True, mode='overwrite')

print(f"Top 3 hours per product category saved to {OUTPUT_PATH}")
