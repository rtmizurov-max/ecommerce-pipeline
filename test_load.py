from src.fetcher import DataFetcher
from src.transformer import DataTransformer
from src.loader import DataLoader

if __name__ == "__main__":
    # Extract
    fetcher = DataFetcher()
    raw_products = fetcher.fetch_products()
    raw_carts = fetcher.fetch_carts()
    raw_users = fetcher.fetch_users()

    # Transform
    transformer = DataTransformer()
    products_df = transformer.transform_products(raw_products)
    events_df = transformer.transform_events(raw_carts, raw_users)
    events_df = transformer.enrich_events(events_df, products_df)

    # Load
    loader = DataLoader()
    products_loaded = loader.load_products(products_df)
    events_loaded = loader.load_events(events_df)

    print(f"\n ETL completed successfully")
    print(f"   Products loaded: {products_loaded}")
    print(f"   Events loaded: {events_loaded}")