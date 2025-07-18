import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pyspark.sql import SparkSession, Window
from app.utils import load_merchants, load_transactions, fill_missing_merchant_name, fill_missing_category, drop_ambiguous_column
from pyspark.sql import functions as F

# Paths
DATA_DIR = '/home/jovyan/data/clean'
REPORTS_DIR = '/home/jovyan/reports'
MERCHANTS_PATH = os.path.join(DATA_DIR, 'merchants_cleaned.parquet')
TRANSACTIONS_PATH = os.path.join(DATA_DIR, 'historical_transactions_cleaned.parquet')
OUTPUT_POPULAR_MERCHANTS = os.path.join(REPORTS_DIR, 'q4_popular_merchants.csv')
OUTPUT_CITY_CATEGORY = os.path.join(REPORTS_DIR, 'q4_city_category_crosstab.csv')

# Start Spark session
spark = SparkSession.builder.getOrCreate()

# Set log level to ERROR to reduce log noise
spark.sparkContext.setLogLevel('ERROR')

# Load cleaned data
merchants = spark.read.parquet(MERCHANTS_PATH)
transactions = spark.read.parquet(TRANSACTIONS_PATH)

# Drop ambiguous city_id and category from merchants before join
merchants = drop_ambiguous_column(merchants, 'city_id')
merchants = drop_ambiguous_column(merchants, 'category')

# Join transactions with merchants on merchant_id
joined = transactions.join(merchants, on='merchant_id', how='left')

# Fill missing merchant name and category
joined = fill_missing_merchant_name(joined)
joined = fill_missing_category(joined)

# PART 1: Most popular merchants per city (by number of sales)
merchant_city_sales = joined.groupBy('city_id', 'merchant', 'category').agg(
    F.count('*').alias('No of sales')
)

# For each city, get the merchant(s) with the most sales
window = Window.partitionBy('city_id').orderBy(F.desc('No of sales'))
merchant_city_sales = merchant_city_sales.withColumn('rank', F.row_number().over(window))
most_popular_merchants = merchant_city_sales.filter(F.col('rank') == 1)

# Save to CSV
most_popular_merchants.select('city_id', 'merchant', 'category', 'No of sales') \
    .orderBy('city_id', F.desc('No of sales')) \
    .coalesce(1).write.csv(OUTPUT_POPULAR_MERCHANTS, header=True, mode='overwrite')

print(f"Most popular merchants per city saved to {OUTPUT_POPULAR_MERCHANTS}")

# PART 2: City vs. Category cross-tab (number of sales per category in each city)
city_category = joined.groupBy('city_id', 'category').agg(
    F.count('*').alias('No of sales')
)

city_category.orderBy('city_id', F.desc('No of sales')) \
    .coalesce(1).write.csv(OUTPUT_CITY_CATEGORY, header=True, mode='overwrite')

print(f"City vs. category cross-tab saved to {OUTPUT_CITY_CATEGORY}")
