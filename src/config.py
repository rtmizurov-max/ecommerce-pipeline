import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()


class Config:
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
        if not cls.DATABASE_URL:
            raise ValueError(
                "DATABASE_URL is not set. "
                "Copy .env.example to .env and configure database connection."
            )

        parsed = urlparse(cls.DATABASE_URL)
        if not parsed.hostname or not parsed.path:
            raise ValueError(f"Invalid DATABASE_URL format: {cls.DATABASE_URL}")

        Path('logs').mkdir(exist_ok=True)
        Path(cls.DATA_LAKE_PATH).mkdir(parents=True, exist_ok=True)

    @classmethod
    def setup_logging(cls):
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

    @classmethod
    def is_docker(cls) -> bool:
        return os.path.exists('/.dockerenv')


Config.validate()
logger = Config.setup_logging()

_host = urlparse(Config.DATABASE_URL).hostname
_mode = "Docker" if Config.is_docker() else "Local"
logger.info(f"Configuration loaded | Database: {_host} | Mode: {_mode}")
