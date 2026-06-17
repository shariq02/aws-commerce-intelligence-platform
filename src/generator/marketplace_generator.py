import time
import logging
import random
import pandas as pd
from base_generator import BaseGenerator

logger = logging.getLogger(__name__)

TOPIC = "marketplace.events"

SLA_THRESHOLDS = {
    "gold": 60,
    "silver": 90,
    "bronze": 120,
    "new": 180,
}

DISPATCH_TIME_PARAMS = {
    "gold": {"mean": 45, "std": 15},
    "silver": {"mean": 75, "std": 20},
    "bronze": {"mean": 100, "std": 25},
    "new": {"mean": 150, "std": 35},
}


class MarketplaceGenerator(BaseGenerator):

    def __init__(self, data_path, event_rate=10, run_id=None):
        super().__init__(
            domain="marketplace",
            source_system="marketplace-simulator",
            event_rate=event_rate,
            run_id=run_id,
        )
        self.data_path = data_path
        self.sellers_df = None
        self.items_df = None
        self.products_df = None
        self.translations_df = None
        self.seller_tiers = {}

    def load_data(self):
        logger.info("Loading Olist marketplace datasets...")
        self.sellers_df = pd.read_csv(
            f"{self.data_path}/olist_sellers_dataset.csv"
        )
        self.items_df = pd.read_csv(
            f"{self.data_path}/olist_order_items_dataset.csv"
        )
        self.products_df = pd.read_csv(
            f"{self.data_path}/olist_products_dataset.csv"
        )
        self.translations_df = pd.read_csv(
            f"{self.data_path}/product_category_name_translation.csv"
        )
        self.products_df = self.products_df.merge(
            self.translations_df,
            on="product_category_name",
            how="left",
        )
        self.products_df["product_category_name_english"] = (
            self.products_df["product_category_name_english"].fillna("other")
        )
        self._assign_seller_tiers()
        logger.info(f"Loaded {len(self.sellers_df)} sellers successfully.")

    def _assign_seller_tiers(self):
        seller_order_counts = (
            self.items_df.groupby("seller_id")["order_id"]
            .count()
            .reset_index()
            .rename(columns={"order_id": "order_count"})
        )
        for _, row in seller_order_counts.iterrows():
            count = row["order_count"]
            if count >= 100:
                tier = "gold"
            elif count >= 50:
                tier = "silver"
            elif count >= 10:
                tier = "bronze"
            else:
                tier = "new"
            self.seller_tiers[row["seller_id"]] = tier

    def _get_dispatch_time(self, tier):
        params = DISPATCH_TIME_PARAMS[tier]
        return max(10, int(random.gauss(params["mean"], params["std"])))

    def _get_category(self, product_id):
        product = self.products_df[
            self.products_df["product_id"] == product_id
        ]
        if len(product) > 0:
            return product["product_category_name_english"].values[0]
        return "other"

    def _publish_listing_created(self, seller_id, item, tier):
        category = self._get_category(item["product_id"])
        price = float(item["price"])
        payload = {
            "listing_id": f"LST-{item['order_id']}-{item['product_id'][:8]}",
            "seller_id": str(seller_id),
            "seller_tier": tier,
            "product_id": str(item["product_id"]),
            "category": category,
            "price": price,
            "currency": "BRL",
            "stock_quantity": random.randint(10, 200),
            "region": f"BR-{item.get('seller_state', 'SP')}",
        }
        event = self.build_envelope(
            event_type="listing.created",
            payload=payload,
            correlation_id=f"LST-{item['order_id']}-{item['product_id'][:8]}",
        )
        self.publish(TOPIC, event, key=str(seller_id))

    def _publish_order_dispatched(self, seller_id, item, tier):
        dispatch_time = self._get_dispatch_time(tier)
        sla_threshold = SLA_THRESHOLDS[tier]
        is_breached = dispatch_time > sla_threshold
        payload = {
            "order_id": str(item["order_id"]),
            "seller_id": str(seller_id),
            "listing_id": f"LST-{item['order_id']}-{item['product_id'][:8]}",
            "dispatch_time_mins": dispatch_time,
            "sla_threshold_mins": sla_threshold,
            "is_sla_breached": is_breached,
            "carrier": "correios",
        }
        event = self.build_envelope(
            event_type="seller.order.dispatched",
            payload=payload,
            correlation_id=f"LST-{item['order_id']}-{item['product_id'][:8]}",
        )
        self.publish(TOPIC, event, key=str(seller_id))

    def _publish_price_updated(self, seller_id, item, tier):
        if random.random() > 0.1:
            return
        old_price = float(item["price"])
        change_pct = round(random.uniform(-25.0, 25.0), 2)
        new_price = round(old_price * (1 + change_pct / 100), 2)
        payload = {
            "listing_id": f"LST-{item['order_id']}-{item['product_id'][:8]}",
            "seller_id": str(seller_id),
            "old_price": old_price,
            "new_price": new_price,
            "change_pct": change_pct,
            "reason": "algorithm",
        }
        event = self.build_envelope(
            event_type="price.updated",
            payload=payload,
            correlation_id=f"LST-{item['order_id']}-{item['product_id'][:8]}",
        )
        self.publish(TOPIC, event, key=str(seller_id))

    def generate(self):
        self.load_data()

        start_row, completed = self.load_checkpoint()
        if completed:
            return

        logger.info(f"Starting marketplace generator at {self.event_rate} events/sec...")
        if start_row > 0:
            logger.info(f"Resuming from row {start_row} of {len(self.items_df)}")

        delay = 1.0 / self.event_rate

        for idx, (_, item) in enumerate(self.items_df.iterrows()):
            if idx < start_row:
                continue

            seller_id = item["seller_id"]
            tier = self.seller_tiers.get(seller_id, "new")
            self._publish_listing_created(seller_id, item, tier)
            self._publish_order_dispatched(seller_id, item, tier)
            self._publish_price_updated(seller_id, item, tier)
            time.sleep(delay)

            if (idx + 1) % self.CHECKPOINT_INTERVAL == 0:
                self.save_checkpoint(idx + 1)
                logger.info(f"Checkpoint saved: row {idx + 1}/{len(self.items_df)}")

        self.flush()
        self.save_checkpoint(len(self.items_df), completed=True)
        logger.info("Marketplace generator completed.")
