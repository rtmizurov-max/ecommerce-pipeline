from src.fetcher import DataFetcher

if __name__ == "__main__":
    fetcher = DataFetcher()
    products = fetcher.fetch_products()
    carts = fetcher.fetch_carts()
    users = fetcher.fetch_users()

    print(f"Products: {len(products)}")
    print(f"Carts: {len(carts)}")
    print(f"Users: {len(users)}")