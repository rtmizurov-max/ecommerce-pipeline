import traceback
from datetime import datetime, timezone
from .config import Config, logger
from .fetcher import DataFetcher
from .transformer import DataTransformer
from .loader import DataLoader


class DataPipeline:
    def __init__(self):
        self.fetcher = DataFetcher()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.start_time = None
        self.status = "pending"

    def run(self) -> dict:
        self.start_time = datetime.now(timezone.utc)
        logger.info("=" * 60)
        logger.info("ЗАПУСК ETL ПАЙПЛАЙНА")
        logger.info("=" * 60)

        try:
            logger.info("\n[1/4] ИЗВЛЕЧЕНИЕ данных из источника...")
            raw_products = self.fetcher.fetch_products()
            raw_carts = self.fetcher.fetch_carts()
            raw_users = self.fetcher.fetch_users()

            logger.info("\n[2/4] ТРАНСФОРМАЦИЯ данных...")
            products_df = self.transformer.transform_products(raw_products)
            events_df = self.transformer.transform_events(raw_carts, raw_users)
            events_df = self.transformer.enrich_events(events_df, products_df)

            logger.info("\n[3/4] ЗАГРУЗКА данных в хранилище...")
            products_loaded = self.loader.load_products(products_df)
            events_loaded = self.loader.load_events(events_df)

            self.status = "success"
            duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()

            logger.info("\n" + "=" * 60)
            logger.info(" ПАЙПЛАЙН УСПЕШНО ЗАВЕРШЁН")
            logger.info("=" * 60)
            logger.info(f"⏱  Длительность: {duration:.1f} сек")
            logger.info(f" Товаров загружено: {products_loaded}")
            logger.info(f" Событий загружено: {events_loaded}")
            logger.info("=" * 60)

            return {
                "status": "success",
                "duration_sec": duration,
                "products_loaded": products_loaded,
                "events_loaded": events_loaded,
                "run_at": self.start_time.isoformat()
            }

        except Exception as e:
            self.status = "failed"
            duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()

            logger.error("\n" + "=" * 60)
            logger.error(" ПАЙПЛАЙН ЗАВЕРШИЛСЯ С ОШИБКОЙ")
            logger.error("=" * 60)
            logger.error(f"Тип ошибки: {type(e).__name__}")
            logger.error(f"Сообщение: {str(e)}")
            logger.error(f"Трассировка:\n{traceback.format_exc()}")
            logger.error("=" * 60)

            return {
                "status": "failed",
                "duration_sec": duration,
                "error": str(e),
                "error_type": type(e).__name__,
                "run_at": self.start_time.isoformat()
            }


if __name__ == "__main__":
    pipeline = DataPipeline()
    result = pipeline.run()
    exit(0 if result["status"] == "success" else 1)
