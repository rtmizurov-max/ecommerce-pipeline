import requests
import json
from datetime import datetime, timezone
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Config, logger


class APIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EcommerceDataPipeline/1.0'
        })

        retries = Retry(
            total=Config.API_RETRY_COUNT,
            backoff_factor=Config.API_RETRY_DELAY,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

    def fetch(self, endpoint: str, params: dict = None):
        url = f"{Config.API_BASE_URL}{endpoint}"
        logger.info(f"Fetching data from API: {url}")

        try:
            response = self.session.get(
                url,
                params=params,
                timeout=Config.API_TIMEOUT
            )
            response.raise_for_status()
            logger.info(f"API response: {response.status_code} ({len(response.content)} bytes)")
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise


class DataFetcher:
    def __init__(self):
        self.client = APIClient()
        self.data_lake_path = Path(Config.DATA_LAKE_PATH)

    def fetch_products(self):
        logger.info("Fetching products...")
        data = self.client.fetch('/products')
        self._save_raw('products', data)
        logger.info(f"Fetched {len(data)} products")
        return data

    def fetch_carts(self):
        logger.info("Fetching carts...")
        data = self.client.fetch('/carts')
        self._save_raw('carts', data)
        logger.info(f"Fetched {len(data)} carts")
        return data

    def fetch_users(self):
        logger.info("Fetching users...")
        data = self.client.fetch('/users')
        self._save_raw('users', data)
        logger.info(f"Fetched {len(data)} users")
        return data

    def _save_raw(self, entity: str, data: list):
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        filename = f"{entity}_{timestamp}.json"
        filepath = self.data_lake_path / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.debug(f"Raw data saved: {filepath}")
        return filepath
