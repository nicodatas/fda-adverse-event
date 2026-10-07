# Databricks notebook source
# DBTITLE 1,Bronze Layer Title
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion from openFDA
# MAGIC
# MAGIC Fetches drug adverse event reports from the openFDA API and lands them as raw JSON files in the bronze volume. Each pipeline execution gets a unique `run_id` so re-runs never overwrite previous ingestion runs. Files are organized as `ingestion_date=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSZ/page_NNN.json`.

# COMMAND ----------

# DBTITLE 1,Bronze — API Fetch
import logging
import os
import time
import json
from pathlib import Path
from datetime import datetime, timezone
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# openFDA drug adverse event endpoint
API_URL = "https://api.fda.gov/drug/event.json"
# use FDA_API_KEY env var for higher rate limits; empty string falls back to unauthenticated
API_KEY = os.environ.get("FDA_API_KEY", "")

# max 1000 records per request is the API hard cap (requires API key)
# without an API key, the API allows up to 500 records per request
PAGE_SIZE = 1000 if API_KEY else 500
MAX_PAGES = 5 if API_KEY else 10  # 5000 records per run either way
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2  # base seconds, doubled each retry

api_params = {"limit": PAGE_SIZE}
if API_KEY:
    api_params["api_key"] = API_KEY

network_headers = {
    "User-Agent": "DatabricksNotebook/1.0",
    "Accept": "application/json"
}

# bronze base dir — each run creates a unique subdirectory so re-runs never overwrite
BRONZE_BASE_DIR = Path("/Volumes/workspace/default/fda_data/bronze")
now = datetime.now(timezone.utc)
ingestion_date_str = now.strftime("%Y-%m-%d")
run_id = now.strftime("%Y%m%dT%H%M%SZ")
OUTPUT_DIR = BRONZE_BASE_DIR / f"ingestion_date={ingestion_date_str}" / f"run_id={run_id}"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"Bronze run_id: {run_id}")


def fetch_page(url: str, params: dict, headers: dict, output_path: str) -> int:
    """Fetch one page from the API, stream to disk, return record count."""
    for attempt in range(RETRY_ATTEMPTS):
        try:
            # stream the response so we don't load 20MB+ into memory
            with requests.get(url, params=params, headers=headers, timeout=30, stream=True) as r:
                r.raise_for_status()
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)

            # quick parse to check how many records we got
            with open(output_path, "r") as f:
                data = json.load(f)
            count = len(data.get("results", []))
            logger.info(f"Page saved: {output_path} ({count} records)")
            return count

        except Exception as e:
            if attempt < RETRY_ATTEMPTS - 1:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"Failed after {RETRY_ATTEMPTS} attempts: {e}")
                raise


def fetch_and_save_raw_data(url: str, base_params: dict, headers: dict, output_dir: Path) -> None:
    logger.info(f"Starting paginated fetch from {url}")
    total_records = 0

    for page in range(MAX_PAGES):
        skip = page * PAGE_SIZE
        params = {**base_params, "skip": skip}
        output_path = output_dir / f"page_{page:03d}.json"

        count = fetch_page(url, params, headers, str(output_path))
        total_records += count

        # fewer records than page size means we've reached the end
        if count < PAGE_SIZE:
            logger.info(f"Reached end of results at page {page}.")
            break

    logger.info(f"Fetch complete: {total_records} records across {page + 1} page(s).")


if __name__ == "__main__":
    fetch_and_save_raw_data(API_URL, api_params, network_headers, OUTPUT_DIR)