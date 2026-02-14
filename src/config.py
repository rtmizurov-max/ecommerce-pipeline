"""
Configuration module for the data pipeline.
Handles environment variables, validation, and logging setup.
"""
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()


class Config:
    """Centralized configuration management."""

    DATABASE_URL = os.getenv('DATABASE_URL')
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'logs/pipeline.log')
    API_BASE_URL = os.getenv('API_BASE_URL', 'https://fakestoreapi.com')
    API_TIMEOUT = int(os.getenv('API_TIMEOUT', '10'))
    API_RETRY_COUNT = int(os.getenv('API_RETRY_COUNT', '3'))
    API_RETRY_DELAY = int(os.getenv('API_RETRY_DELAY', '2'))
    DATA_LAKE_PATH = os.getenv('DATA_LAKE_PATH', 'data_lake/raw')

    @classmethod
    def validate(cls):
        """Validate critical configuration parameters."""
        if not cls.DATABASE_URL:
            raise ValueError(
                "DATABASE_URL is not set.\n"
                "1. Register at https://supabase.com\n"
                "2. Create project → Settings → Database → Connection string\n"
                "3. Create .env file and paste the connection string"
            )

        parsed = urlparse(cls.DATABASE_URL)
        if not parsed.hostname or not parsed.path:
            raise ValueError(f"Invalid DATABASE_URL format: {cls.DATABASE_URL}")

        Path('logs').mkdir(exist_ok=True)
        Path(cls.DATA_LAKE_PATH).mkdir(parents=True, exist_ok=True)

    @classmethod
    def setup_logging(cls):
        """Configure production-ready logging."""
        logging.basicConfig(
            level=getattr(logging, cls.LOG_LEVEL),
            format='%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(cls.LOG_FILE, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger('pipeline')


Config.validate()
logger = Config.setup_logging()
logger.info(f"Configuration loaded. Database host: {urlparse(Config.DATABASE_URL).hostname}")