import os
from pyspark.sql import SparkSession
from app.utils import load_merchants, load_transactions
from app.cleaning import clean_dataframe

RAW_DATA_DIR = '/home/jovyan/data/in'
CLEAN_DATA_DIR = '/home/jovyan/data/clean'
os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

MERCHANTS_PATH = os.path.join(RAW_DATA_DIR, 'merchants-subset.csv')
TRANSACTIONS_PATH = os.path.join(RAW_DATA_DIR, 'historical_transactions.parquet')
MERCHANTS_CLEAN_PATH = os.path.join(CLEAN_DATA_DIR, 'merchants_cleaned.parquet')
TRANSACTIONS_CLEAN_PATH = os.path.join(CLEAN_DATA_DIR, 'historical_transactions_cleaned.parquet')

def main():
    spark = SparkSession.builder.getOrCreate()
    print('Loading raw merchants...')
    merchants = load_merchants(spark, MERCHANTS_PATH)
    print('Cleaning merchants...')
    merchants_clean = clean_dataframe(
        merchants,
        string_columns=['merchant_name', 'merchant_id', 'category', 'city_id'],
        dedup_keys=['merchant_id'],
        cache=False
    )
    print(f'Saving cleaned merchants to {MERCHANTS_CLEAN_PATH}')
    merchants_clean.write.mode('overwrite').parquet(MERCHANTS_CLEAN_PATH)

    print('Loading raw transactions...')
    transactions = load_transactions(spark, TRANSACTIONS_PATH)
    print('Cleaning transactions...')
    transactions_clean = clean_dataframe(
        transactions,
        string_columns=['merchant_id', 'category', 'city_id'],
        dedup_keys=None,  # deduplicate on full row
        cache=False
    )
    print(f'Saving cleaned transactions to {TRANSACTIONS_CLEAN_PATH}')
    transactions_clean.write.mode('overwrite').parquet(TRANSACTIONS_CLEAN_PATH)
    print('Preprocessing complete.')

if __name__ == '__main__':
    main() 