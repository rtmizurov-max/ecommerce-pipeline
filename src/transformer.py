import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict
import random
from .config import logger


class DataTransformer:

    @staticmethod
    def transform_products(raw_products: List[Dict]) -> pd.DataFrame:
        logger.info("Transforming products data...")

        products = []
        for p in raw_products:
            try:
                products.append({
                    "product_id": int(p["id"]),
                    "title": str(p["title"]),
                    "price": float(p["price"]),
                    "category": str(p["category"]),
                    "rating": float(p["rating"]["rate"]),
                    "rating_count": int(p["rating"]["count"]),
                    "loaded_at": datetime.now(timezone.utc)
                })
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid product {p.get('id', 'unknown')}: {e}")
                continue

        df = pd.DataFrame(products)
        logger.info(f"Transformed {len(df)} products")
        return df


    @staticmethod
    def transform_events(raw_carts: List[Dict], raw_users: List[Dict]) -> pd.DataFrame:
        logger.info("Generating realistic event funnel...")

        events = []
        user_map = {u["id"]: u for u in raw_users}

        for cart in raw_carts:
            try:
                cart_id = cart["id"]
                user_id = cart["userId"]
                cart_date = pd.to_datetime(cart["date"], utc=True)

                user = user_map.get(user_id, {})
                user_city = user.get("address", {}).get("city", "Unknown")

                for item in cart["products"]:
                    product_id = int(item["productId"])
                    quantity = int(item["quantity"])

                    view_time = cart_date - timedelta(minutes=random.randint(5, 30))

                    # VIEW — всегда
                    events.append({
                        "event_id": f"view_{cart_id}_{product_id}_{random.randint(1, 9999)}",
                        "user_id": user_id,
                        "product_id": product_id,
                        "event_type": "view",
                        "quantity": 1,
                        "event_time": view_time,
                        "user_city": user_city,
                        "session_id": f"sess_{cart_id}"
                    })

                    # ADD TO CART — 60% случаев
                    if random.random() < 0.6:
                        events.append({
                            "event_id": f"cart_{cart_id}_{product_id}_{random.randint(1, 9999)}",
                            "user_id": user_id,
                            "product_id": product_id,
                            "event_type": "add_to_cart",
                            "quantity": quantity,
                            "event_time": cart_date,
                            "user_city": user_city,
                            "session_id": f"sess_{cart_id}"
                        })

                        # PURCHASE — 30% случаев
                        if random.random() < 0.5:
                            purchase_time = cart_date + timedelta(minutes=random.randint(1, 10))

                            events.append({
                                "event_id": f"purchase_{cart_id}_{product_id}_{random.randint(1, 9999)}",
                                "user_id": user_id,
                                "product_id": product_id,
                                "event_type": "purchase",
                                "quantity": quantity,
                                "event_time": purchase_time,
                                "user_city": user_city,
                                "session_id": f"sess_{cart_id}"
                            })

            except Exception as e:
                logger.warning(f"Skipping cart {cart.get('id')}: {e}")
                continue

        df = pd.DataFrame(events)
        df["event_date"] = df["event_time"].dt.date

        logger.info(f"Generated {len(df)} events")
        logger.info(df["event_type"].value_counts().to_string())

        return df

    @staticmethod
    def enrich_events(events_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Enriching events with product attributes...")

        # Приводим типы
        events_df["product_id"] = events_df["product_id"].astype(int)
        products_df["product_id"] = products_df["product_id"].astype(int)

        # Merge
        enriched = events_df.merge(
            products_df[["product_id", "price", "category"]],
            on="product_id",
            how="left"
        )

        # Проверка на проблемы join
        missing_prices = enriched["price"].isna().sum()
        if missing_prices > 0:
            logger.warning(f"{missing_prices} events without price after enrichment")

        # Финальная обработка
        enriched["price"] = enriched["price"].fillna(0.0)
        enriched["category"] = enriched["category"].fillna("unknown")

        column_order = [
            "event_id",
            "user_id",
            "product_id",
            "event_type",
            "quantity",
            "price",
            "category",
            "event_time",
            "event_date",
            "user_city",
            "session_id"
        ]

        enriched = enriched[column_order]

        logger.info("Event enrichment completed successfully")

        return enriched
