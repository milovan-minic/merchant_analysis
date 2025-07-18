import os
import requests
from tqdm import tqdm

DATA_IN_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'in')
os.makedirs(DATA_IN_DIR, exist_ok=True)


def download_file(url, dest_filename, chunk_size=1024 * 1024):
    """
    Download a file from a URL to a local destination using streaming.
    Shows a progress bar for large files.
    """
    dest_path = os.path.join(DATA_IN_DIR, dest_filename)
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total = int(response.headers.get('content-length', 0))
    with open(dest_path, 'wb') as file, tqdm(
        desc=dest_filename,
        total=total,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=chunk_size):
            size = file.write(data)
            bar.update(size)
    return dest_path


def download_google_sheet_as_xlsx(sheet_url, dest_filename):
    """
    Download a Google Sheet as an Excel file (.xlsx) given its share URL.
    """
    import re
    match = re.search(r"/d/([a-zA-Z0-9-_]+)", sheet_url)
    if not match:
        raise ValueError("Invalid Google Sheet URL")
    sheet_id = match.group(1)
    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    return download_file(export_url, dest_filename)


if __name__ == "__main__":
    files = [
        {
            "url": "https://billups-tech-interview.s3.us-west-2.amazonaws.com/engineering/data-engineering-task/interview_historical_transactions.parquet/part-00000-tid-860771939793626614-979f966a-6d53-4896-9692-f81194d27b99-109986-1-c000.snappy.parquet",
            "dest": "historical_transactions.parquet"
        },
        {
            "url": "https://billups-tech-interview.s3.us-west-2.amazonaws.com/engineering/data-engineering-task/merchants-subset.csv",
            "dest": "merchants-subset.csv"
        }
    ]
    for f in files:
        print(f"Downloading {f['dest']}...")
        download_file(f["url"], f["dest"])
    print("Downloading Data Dictionary as Excel...")
    download_google_sheet_as_xlsx(
        "https://docs.google.com/spreadsheets/d/1k05tl92SY5CvEwa0-nFOQMOjkLgZOL9qTXsDsh0F-_Q/edit?usp=sharing",
        "data_dictionary.xlsx"
    )
    print("All files downloaded.") 