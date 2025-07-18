import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pyspark.sql import SparkSession
from app.utils import load_merchants, load_transactions, fill_missing_merchant_name, drop_ambiguous_column
from pyspark.sql import functions as F

# Paths
DATA_DIR = '/home/jovyan/data/clean'
REPORTS_DIR = '/home/jovyan/reports'
MERCHANTS_PATH = os.path.join(DATA_DIR, 'merchants_cleaned.parquet')
TRANSACTIONS_PATH = os.path.join(DATA_DIR, 'historical_transactions_cleaned.parquet')
OUTPUT_PATH = os.path.join(REPORTS_DIR, 'q2_avg_sale.csv')

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Set log level to ERROR to reduce log noise
spark.sparkContext.setLogLevel('ERROR')

# Load cleaned data
merchants = spark.read.parquet(MERCHANTS_PATH)
transactions = spark.read.parquet(TRANSACTIONS_PATH)

# Drop ambiguous state_id from merchants before join
merchants = drop_ambiguous_column(merchants, 'state_id')

# Join transactions with merchants on merchant_id
joined = transactions.join(merchants, on='merchant_id', how='left')

# Fill missing merchant name
joined = fill_missing_merchant_name(joined)

# Group by merchant and state_id, calculate average purchase_amount
agg = joined.groupBy('merchant', 'state_id').agg(
    F.avg('purchase_amount').alias('Average Amount')
)

# Order by average amount descending
result = agg.orderBy(F.desc('Average Amount'))

# Save to CSV
result.coalesce(1).write.csv(OUTPUT_PATH, header=True, mode='overwrite')

print(f"Average sale amount per merchant and state saved to {OUTPUT_PATH}")
