"""
Data loading module for idempotent database operations.
Handles schema creation, conflict resolution, and index management.
"""
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from .config import Config, logger


class DataLoader:
    """
    Loads transformed data into PostgreSQL with idempotency guarantees.
    Uses ON CONFLICT clauses to prevent duplicate records.
    """

    def __init__(self):
        self.engine = create_engine(
            Config.DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        self._create_schema()

    def _create_schema(self):
        """Create database schema with optimized indexes for analytical queries."""
        logger.info("Creating database schema...")

        with self.engine.connect() as conn:
            # Products table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    category TEXT NOT NULL,
                    rating DECIMAL(3,2) NOT NULL,
                    rating_count INTEGER NOT NULL,
                    loaded_at TIMESTAMP WITH TIME ZONE NOT NULL
                )
            """))

            # Events table (core analytical table)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    product_id INTEGER,
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

            # Indexes for analytical performance
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_user ON events(user_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_city ON events(user_city)"))

            conn.commit()

        logger.info("Database schema created successfully")

    def load_products(self, df: pd.DataFrame) -> int:
        """
        Idempotently load products with upsert semantics.

        Args:
            df: DataFrame with product data

        Returns:
            Number of rows inserted/updated
        """
        logger.info(f"Loading {len(df)} products into database...")

        try:
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
        """
        Idempotently load events with skip-on-conflict semantics.

        Args:
            df: DataFrame with event data

        Returns:
            Number of new events inserted (duplicates skipped)
        """
        logger.info(f"Loading {len(df)} events into database...")

        try:
            # Prepare records with explicit loaded_at timestamp
            df_load = df.copy()
            df_load['loaded_at'] = pd.Timestamp.now(tz='UTC')

            insert_query = """
                INSERT INTO events 
                    (event_id, user_id, product_id, event_type, quantity, price, 
                     category, event_time, event_date, user_city, session_id, loaded_at)
                VALUES (:event_id, :user_id, :product_id, :event_type, :quantity, :price, 
                        :category, :event_time, :event_date, :user_city, :session_id, :loaded_at)
                ON CONFLICT (event_id) DO NOTHING
            """

            records = df_load.to_dict('records')
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