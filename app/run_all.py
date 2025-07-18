import subprocess
import sys

scripts = [
    'app.preprocess_data',
    'app.analysis.q1_top_merchants',
    'app.analysis.q2_avg_sale',
    'app.analysis.q3_top_hours',
    'app.analysis.q4_popular_merchants',
    'app.analysis.q5_advice',
]

def run_script(module):
    print(f'\n===== Running {module} =====')
    try:
        subprocess.run([sys.executable, '-m', module], check=True)
    except subprocess.CalledProcessError as e:
        print(f'Error running {module}:', e)
        sys.exit(1)

if __name__ == '__main__':
    for script in scripts:
        run_script(script)
    print('\nAll preprocessing and analysis scripts completed successfully.')
