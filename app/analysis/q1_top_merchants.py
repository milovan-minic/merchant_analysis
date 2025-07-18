import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pyspark.sql import SparkSession
from app.utils import load_merchants, load_transactions, fill_missing_merchant_name, add_month_column, drop_ambiguous_column
from pyspark.sql import functions as F

# Paths
DATA_DIR = '/home/jovyan/data/clean'
REPORTS_DIR = '/home/jovyan/reports'
MERCHANTS_PATH = os.path.join(DATA_DIR, 'merchants_cleaned.parquet')
TRANSACTIONS_PATH = os.path.join(DATA_DIR, 'historical_transactions_cleaned.parquet')
OUTPUT_PATH = os.path.join(REPORTS_DIR, 'q1_top_merchants.csv')

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Set log level to ERROR to reduce log noise
spark.sparkContext.setLogLevel('ERROR')

# Load cleaned data
merchants = spark.read.parquet(MERCHANTS_PATH)
transactions = spark.read.parquet(TRANSACTIONS_PATH)

# Join transactions with merchants on merchant_id
joined = transactions.join(merchants, on='merchant_id', how='left')

# Drop ambiguous city_id from merchants
joined = drop_ambiguous_column(joined, 'city_id')

# Fill missing merchant name
joined = fill_missing_merchant_name(joined)

# Add month column
joined = add_month_column(joined)

# Group by merchant, aggregate purchase total and number of sales
agg = joined.groupBy('merchant').agg(
    F.sum('purchase_amount').alias('Purchase Total'),
    F.count('*').alias('No of sales')
)

# Rank merchants by purchase total
agg = agg.orderBy(F.desc('Purchase Total'))

# Select top 5 merchants
top5 = agg.limit(5)

# Save to CSV
top5.coalesce(1).write.csv(OUTPUT_PATH, header=True, mode='overwrite')

print(f"Top 5 merchants overall saved to {OUTPUT_PATH}")
