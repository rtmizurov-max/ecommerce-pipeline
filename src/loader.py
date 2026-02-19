import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .config import Config, logger


class DataLoader:
    def __init__(self):
        self.engine = create_engine(
            Config.DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        self._create_schema()

    def _create_schema(self):
        logger.info("Creating database schema...")

        with self.engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id BIGINT PRIMARY KEY,
                    title TEXT NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    category TEXT NOT NULL,
                    rating DECIMAL(3,2) NOT NULL,
                    rating_count INTEGER NOT NULL,
                    loaded_at TIMESTAMP WITH TIME ZONE NOT NULL
                )
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    product_id BIGINT,
                    event_type TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    category TEXT,
                    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    event_date DATE NOT NULL,
                    user_city TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                )
            """))

            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_city ON events(user_city)"))

            conn.commit()

        logger.info("Database schema created successfully")

    def load_products(self, df: pd.DataFrame) -> int:
        logger.info(f"Loading {len(df)} products into database...")

        try:
            df = df.copy()
            df['product_id'] = df['product_id'].astype('Int64')

            insert_query = """
                INSERT INTO products 
                    (product_id, title, price, category, rating, rating_count, loaded_at)
                VALUES (:product_id, :title, :price, :category, :rating, :rating_count, :loaded_at)
                ON CONFLICT (product_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    category = EXCLUDED.category,
                    rating = EXCLUDED.rating,
                    rating_count = EXCLUDED.rating_count,
                    loaded_at = EXCLUDED.loaded_at
            """

            records = df.to_dict('records')
            with self.engine.connect() as conn:
                result = conn.execute(text(insert_query), records)
                conn.commit()

            logger.info(f"Loaded {result.rowcount} products (upserted existing records)")
            return result.rowcount

        except SQLAlchemyError as e:
            logger.error(f"Database error during products load: {e}")
            raise

    def load_events(self, df: pd.DataFrame) -> int:
        logger.info(f"Loading {len(df)} events into database...")

        try:
            df = df.copy()

            df['user_id'] = df['user_id'].astype('Int64')
            df['product_id'] = df['product_id'].astype('Int64')

            df['product_id'] = df['product_id'].where(pd.notnull(df['product_id']), None)
            df['user_id'] = df['user_id'].where(pd.notnull(df['user_id']), None)

            df['loaded_at'] = pd.Timestamp.now(tz='UTC')

            insert_query = """
                INSERT INTO events 
                    (event_id, user_id, product_id, event_type, quantity, price, 
                     category, event_time, event_date, user_city, session_id, loaded_at)
                VALUES (:event_id, :user_id, :product_id, :event_type, :quantity, :price, 
                        :category, :event_time, :event_date, :user_city, :session_id, :loaded_at)
                ON CONFLICT (event_id) DO NOTHING
            """

            records = df.to_dict('records')
            with self.engine.connect() as conn:
                result = conn.execute(text(insert_query), records)
                conn.commit()

            inserted = result.rowcount
            skipped = len(df) - inserted

            logger.info(f"Inserted {inserted} new events, skipped {skipped} duplicates")
            return inserted

        except SQLAlchemyError as e:
            logger.error(f"Database error during events load: {e}")
            raise
