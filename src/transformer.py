"""
Data transformation module for e-commerce pipeline.
Converts raw API data into analytical event model with funnel stages.
"""
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict

from .config import logger


class DataTransformer:
    """
    Transforms raw e-commerce data into analytical event model.
    Handles data validation, enrichment, and funnel event generation.
    """

    @staticmethod
    def transform_products(raw_products: List[Dict]) -> pd.DataFrame:
        """
        Transform raw product data into clean analytical table.

        Args:
            raw_products: List of product dictionaries from API

        Returns:
            DataFrame with validated product attributes
        """
        logger.info("Transforming products data...")

        products = []
        for p in raw_products:
            try:
                product = {
                    'product_id': int(p['id']),
                    'title': str(p['title']),
                    'price': float(p['price']),
                    'category': str(p['category']),
                    'rating': float(p['rating']['rate']),
                    'rating_count': int(p['rating']['count']),
                    'loaded_at': datetime.now(timezone.utc)
                }
                products.append(product)
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid product {p.get('id', 'unknown')}: {e}")
                continue

        df = pd.DataFrame(products)
        logger.info(f"Transformed {len(df)} valid products out of {len(raw_products)} raw records")
        return df

    @staticmethod
    def transform_events(raw_carts: List[Dict], raw_users: List[Dict]) -> pd.DataFrame:
        """
        Generate event funnel from raw cart data.

        Creates three event types per cart:
        1. view - simulated product view (5-15 min before cart)
        2. add_to_cart - actual cart addition
        3. purchase - completed transaction (1-3 min after cart)

        Args:
            raw_carts: List of cart dictionaries from API
            raw_users: List of user dictionaries from API

        Returns:
            DataFrame with event funnel records
        """
        logger.info("Generating event funnel from cart data...")

        events = []
        user_map = {u['id']: u for u in raw_users}

        for cart in raw_carts:
            try:
                cart_id = cart['id']
                user_id = cart['userId']
                cart_date = pd.to_datetime(cart['date'], utc=True)

                # Get user location for geo-analysis
                user = user_map.get(user_id, {})
                user_city = user.get('address', {}).get('city', 'Unknown')

                # Generate events for each product in cart
                for item in cart['products']:
                    product_id = item['productId']
                    quantity = item['quantity']

                    # Event 1: Product view (simulated 5-15 min before cart)
                    view_time = cart_date - timedelta(
                        minutes=(cart_id % 11) + 5  # Deterministic but varied timing
                    )
                    events.append({
                        'event_id': f"view_{cart_id}_{product_id}",
                        'user_id': user_id,
                        'product_id': product_id,
                        'event_type': 'view',
                        'quantity': 1,
                        'price': None,
                        'event_time': view_time,
                        'user_city': user_city,
                        'session_id': f"sess_{cart_id}"
                    })

                    # Event 2: Add to cart
                    events.append({
                        'event_id': f"cart_{cart_id}_{product_id}",
                        'user_id': user_id,
                        'product_id': product_id,
                        'event_type': 'add_to_cart',
                        'quantity': quantity,
                        'price': None,
                        'event_time': cart_date,
                        'user_city': user_city,
                        'session_id': f"sess_{cart_id}"
                    })

                # Event 3: Purchase (1-3 min after cart)
                purchase_time = cart_date + timedelta(
                    minutes=(cart_id % 3) + 1
                )
                events.append({
                    'event_id': f"purchase_{cart_id}",
                    'user_id': user_id,
                    'product_id': None,
                    'event_type': 'purchase',
                    'quantity': sum(i['quantity'] for i in cart['products']),
                    'price': None,
                    'event_time': purchase_time,
                    'user_city': user_city,
                    'session_id': f"sess_{cart_id}"
                })

            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid cart {cart.get('id', 'unknown')}: {e}")
                continue

        df = pd.DataFrame(events)
        df['event_date'] = df['event_time'].dt.date

        logger.info(f"Generated {len(df)} events from {len(raw_carts)} carts")
        logger.info(f"Event type distribution:\n{df['event_type'].value_counts().to_string()}")
        return df

    @staticmethod
    def enrich_events(events_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich event data with product attributes (price, category).

        Args:
            events_df: DataFrame with raw events
            products_df: DataFrame with product catalog

        Returns:
            Enriched events DataFrame with price and category columns
        """
        logger.info("Enriching events with product attributes...")

        # Create product lookup table
        product_lookup = products_df[['product_id', 'price', 'category']].set_index('product_id')

        # Merge price and category for events with product_id
        enriched = events_df.merge(
            product_lookup,
            left_on='product_id',
            right_index=True,
            how='left',
            suffixes=('', '_prod')
        )

        # Handle purchase events (no product_id) - assign average order value
        purchase_mask = enriched['event_type'] == 'purchase'
        if purchase_mask.any():
            avg_order_value = 150.0  # Reasonable average for demo dataset
            enriched.loc[purchase_mask, 'price'] = avg_order_value

        # Fill missing values
        enriched['price'] = enriched['price'].fillna(0.0)
        enriched['category'] = enriched['category'].fillna('unknown')

        # Reorder columns for consistency
        column_order = [
            'event_id', 'user_id', 'product_id', 'event_type', 'quantity',
            'price', 'category', 'event_time', 'event_date', 'user_city', 'session_id'
        ]
        enriched = enriched[column_order]

        logger.info("Event enrichment completed")
        return enriched