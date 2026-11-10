# Databricks notebook source
# DBTITLE 1,Bronze Layer Title
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion from openFDA
# MAGIC
# MAGIC Fetches drug adverse event reports from the openFDA API and lands them as raw JSON files in the bronze
# MAGIC volume. Each pipeline execution gets a unique `run_id` so re-runs never overwrite previous ingestion
# MAGIC runs. Files are organized as `ingestion_date=YYYY-MM-DD/run_id=YYYYMMDDTHHMMSSZ/page_NNN.json`.
# MAGIC
# MAGIC **Pagination:** openFDA signals the end of results with HTTP 404 ("No matches found") once `skip`
# MAGIC goes past the last record. Past the first page, 404 is treated as a clean end-of-pagination; on the
# MAGIC first page it is a configuration error. A short page (fewer records than the page size) also
# MAGIC terminates pagination.
# MAGIC
# MAGIC **API key security:** openFDA only accepts the key as a URL query parameter, so `requests`
# MAGIC exception messages embed the key. Every exception message is therefore redacted before logging or
# MAGIC re-raising, and the original `requests` exception is never re-raised.

# COMMAND ----------

# DBTITLE 1,Bronze — API Fetch
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

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
REQUEST_TIMEOUT = 30  # seconds per HTTP request

REDACTED = "***REDACTED***"

api_params = {"limit": PAGE_SIZE}
if API_KEY:
    api_params["api_key"] = API_KEY

network_headers = {
    "User-Agent": "DatabricksNotebook/1.0",
    "Accept": "application/json",
}

# bronze base dir — each run creates a unique subdirectory so re-runs never overwrite
BRONZE_BASE_DIR = Path("/Volumes/workspace/default/fda_data/bronze")
now = datetime.now(timezone.utc)
ingestion_date_str = now.strftime("%Y-%m-%d")
run_id = now.strftime("%Y%m%dT%H%M%SZ")
OUTPUT_DIR = BRONZE_BASE_DIR / f"ingestion_date={ingestion_date_str}" / f"run_id={run_id}"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logger.info(f"Bronze run_id: {run_id}")


def redact_secrets(text: str, secrets: list) -> str:
    """Replace every occurrence of each secret value with a redaction marker."""
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTED)
    return text


def active_secrets() -> list:
    """Secrets currently in play — only the API key, when configured."""
    return [API_KEY] if API_KEY else []


def _is_retryable(status) -> bool:
    """Network errors (no status) and 429/5xx are retryable; other 4xx are not."""
    return status is None or status == 429 or status >= 500


def fetch_page(url: str, params: dict, headers: dict, output_path: str, page_number: int) -> int:
    """Fetch one page from the API, stream it to disk, return the record count.

    Returns 0 for a legitimate end-of-results signal (HTTP 404 past the first
    page, or an empty results array). Raises RuntimeError with a sanitized
    message when the page cannot be fetched. The original requests exception is
    deliberately never re-raised: its message embeds the full request URL, which
    contains the api_key query parameter.
    """
    secrets = active_secrets()
    last_error = "unknown error"
    for attempt in range(RETRY_ATTEMPTS):
        try:
            with requests.get(url, params=params, headers=headers,
                              timeout=REQUEST_TIMEOUT, stream=True) as r:
                if r.status_code == 404:
                    # openFDA answers 404 "No matches found" once skip goes past
                    # the last result — a legitimate end-of-pagination signal.
                    # On the FIRST page it means the endpoint or query is wrong.
                    if page_number == 0:
                        raise RuntimeError(
                            "openFDA returned 404 on the first page — check the endpoint "
                            f"and query parameters. URL: {redact_secrets(url, secrets)}"
                        )
                    logger.info(f"HTTP 404 on page {page_number} — end of results.")
                    return 0
                # 429 (rate limit) and 5xx (server) raise here and are retried;
                # any other 4xx is a permanent client error and is NOT retried.
                r.raise_for_status()

                # stream the response so we don't load 20MB+ into memory
                with open(output_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=65536):
                        f.write(chunk)

            with open(output_path, "r") as f:
                data = json.load(f)
            results = data.get("results")
            if not isinstance(results, list):
                raise RuntimeError(
                    "openFDA response has an unexpected structure: 'results' is "
                    f"{type(results).__name__}, expected a list. Page: {output_path}"
                )
            count = len(results)
            logger.info(f"Page saved: {output_path} ({count} records)")
            return count

        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"openFDA returned a page that is not valid JSON: {output_path} ({e})"
            ) from None

        except requests.exceptions.RequestException as e:
            # requests embeds the full URL (incl. api_key) in exception messages
            message = redact_secrets(str(e), secrets)
            response = getattr(e, "response", None)
            status = getattr(response, "status_code", None)
            last_error = message
            if not _is_retryable(status):
                logger.error(f"Non-retryable API error (HTTP {status}): {message}")
                raise RuntimeError(
                    f"Non-retryable API error (HTTP {status}): {message}"
                ) from None
            if attempt < RETRY_ATTEMPTS - 1:
                wait = RETRY_BACKOFF * (2 ** attempt)
                logger.warning(
                    f"Attempt {attempt + 1} failed (retryable): {message}. Retrying in {wait}s..."
                )
                time.sleep(wait)
            else:
                logger.error(f"Failed after {RETRY_ATTEMPTS} attempts: {message}")
    raise RuntimeError(f"Failed after {RETRY_ATTEMPTS} attempts: {last_error}") from None


def fetch_and_save_raw_data(url: str, base_params: dict, headers: dict,
                            output_dir: Path) -> int:
    """Fetch pages until end of results or MAX_PAGES, whichever comes first."""
    logger.info(f"Starting paginated fetch from {url}")
    total_records = 0
    pages_fetched = 0

    for page in range(MAX_PAGES):
        skip = page * PAGE_SIZE
        params = {**base_params, "skip": skip}
        output_path = output_dir / f"page_{page:03d}.json"

        count = fetch_page(url, params, headers, str(output_path), page)
        pages_fetched += 1

        # 0 records = explicit end of results (404 past the end, or empty page)
        if count == 0:
            logger.info(f"End of results at page {page}.")
            break
        total_records += count

        # fewer records than page size means we've reached the end
        if count < PAGE_SIZE:
            logger.info(f"Reached end of results at page {page}.")
            break

    logger.info(f"Fetch complete: {total_records} records across {pages_fetched} page(s).")
    if total_records == 0:
        logger.warning("Ingestion run captured 0 records — check the API response.")
    return total_records


if __name__ == "__main__":
    fetch_and_save_raw_data(API_URL, api_params, network_headers, OUTPUT_DIR)