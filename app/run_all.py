import subprocess
import sys
import os

scripts = [
    'app.preprocess_data',
    'app.analysis.q1_top_merchants',
    'app.analysis.q2_avg_sale',
    'app.analysis.q3_top_hours',
    'app.analysis.q4_popular_merchants',
    'app.analysis.q5_advice',
]

REQUIRED_FILES = [
    'data/in/merchants-subset.csv',
    'data/in/historical_transactions.parquet',
    'data/in/data_dictionary.xlsx',
]

def run_script(module):
    print(f'\n===== Running {module} =====')
    try:
        subprocess.run([sys.executable, '-m', module], check=True)
    except subprocess.CalledProcessError as e:
        print(f'Error running {module}:', e)
        sys.exit(1)

if __name__ == '__main__':
    missing = [f for f in REQUIRED_FILES if not os.path.exists(f)]
    if missing:
        print("Some required data files are missing. Downloading data...")
        subprocess.run([sys.executable, os.path.join('app', 'download_data.py')], check=True)
    for script in scripts:
        run_script(script)
    print('\nAll preprocessing and analysis scripts completed successfully.')
