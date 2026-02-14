from src.fetcher import DataFetcher
from src.transformer import DataTransformer

if __name__ == "__main__":
    # Fetch raw data
    fetcher = DataFetcher()
    raw_products = fetcher.fetch_products()
    raw_carts = fetcher.fetch_carts()
    raw_users = fetcher.fetch_users()

    # Transform data
    transformer = DataTransformer()
    products_df = transformer.transform_products(raw_products)
    events_df = transformer.transform_events(raw_carts, raw_users)
    events_df = transformer.enrich_events(events_df, products_df)

    # Display results
    print("\nProducts DataFrame:")
    print(f"Shape: {products_df.shape}")
    print(products_df.head(3).to_string(index=False))

    print("\nEvents DataFrame:")
    print(f"Shape: {events_df.shape}")
    print(f"Date range: {events_df['event_date'].min()} to {events_df['event_date'].max()}")
    print(f"Event types:\n{events_df['event_type'].value_counts().to_string()}")
    print("\nSample events:")
    print(events_df[['event_id', 'event_type', 'user_id', 'product_id', 'price']].head(10).to_string(index=False))