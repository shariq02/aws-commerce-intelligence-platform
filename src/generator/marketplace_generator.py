import time
import logging
import random
import pandas as pd
from base_generator import BaseGenerator

logger = logging.getLogger(__name__)

TOPIC = "marketplace.events"

# Fix applied June 2026:
# SLA thresholds and dispatch times now in minutes aligned with real-world
# e-commerce shipping days. Previous values (45-180 mins) were unrealistic
# causing 91% SLA breach rate. Real-world: platinum = 2 day SLA, new = 14 day SLA.

# SLA thresholds in minutes -- realistic day-based targets
SLA_THRESHOLDS = {
    "platinum": 2  * 1440,   # 2 days  = 2,880 mins
    "gold":     3  * 1440,   # 3 days  = 4,320 mins
    "standard": 5  * 1440,   # 5 days  = 7,200 mins
    "silver":   7  * 1440,   # 7 days  = 10,080 mins
    "bronze":   10 * 1440,   # 10 days = 14,400 mins
    "new":      14 * 1440,   # 14 days = 20,160 mins
}

# Dispatch time params in minutes -- realistic e-commerce shipping times
# Mean and std in minutes, corresponding to real day ranges
DISPATCH_TIME_PARAMS = {
    "platinum": {"mean": 2  * 1440, "std": 720},    # avg 2 days  +/- 0.5 days
    "gold":     {"mean": 3  * 1440, "std": 1440},   # avg 3 days  +/- 1 day
    "standard": {"mean": 7  * 1440, "std": 2880},   # avg 7 days  +/- 2 days
    "silver":   {"mean": 10 * 1440, "std": 2880},   # avg 10 days +/- 2 days
    "bronze":   {"mean": 12 * 1440, "std": 4320},   # avg 12 days +/- 3 days
    "new":      {"mean": 15 * 1440, "std": 4320},   # avg 15 days +/- 3 days
}

# Tier thresholds by order count
TIER_THRESHOLDS = {
    "platinum": 200,
    "gold":     100,
    "standard": 20,
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
        self.seller_states = {}
        self.seller_regions = {}

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
        self._load_seller_locations()
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
            if count >= TIER_THRESHOLDS["platinum"]:
                tier = "platinum"
            elif count >= TIER_THRESHOLDS["gold"]:
                tier = "gold"
            elif count >= TIER_THRESHOLDS["standard"]:
                tier = "standard"
            else:
                tier = "new"
            self.seller_tiers[row["seller_id"]] = tier

    def _load_seller_locations(self):
        REGION_MAP = {
            "SP": "southeast", "RJ": "southeast", "MG": "southeast", "ES": "southeast",
            "RS": "south",     "PR": "south",     "SC": "south",
            "BA": "northeast", "PE": "northeast", "CE": "northeast", "MA": "northeast",
            "PA": "northeast", "PB": "northeast", "RN": "northeast", "AL": "northeast",
            "SE": "northeast", "PI": "northeast",
            "DF": "center_west", "GO": "center_west", "MT": "center_west", "MS": "center_west",
            "AM": "north", "RO": "north", "AC": "north", "RR": "north",
            "AP": "north", "TO": "north",
        }
        for _, row in self.sellers_df.iterrows():
            seller_id = row["seller_id"]
            state = str(row.get("seller_state", "SP"))
            self.seller_states[seller_id] = state
            self.seller_regions[seller_id] = REGION_MAP.get(state, "other")

    def _get_dispatch_time(self, tier):
        params = DISPATCH_TIME_PARAMS.get(tier, DISPATCH_TIME_PARAMS["new"])
        # Minimum 1 day (1440 mins) -- no same-day dispatch in this dataset
        return max(1440, int(random.gauss(params["mean"], params["std"])))

    def _get_category(self, product_id):
        product = self.products_df[
            self.products_df["product_id"] == product_id
        ]
        if len(product) > 0:
            cat = product["product_category_name_english"].values[0]
            group = product["product_category_name"].values[0]
            return cat, group
        return "other", "other"

    def _get_dispatch_speed_bucket(self, dispatch_time_days):
        if dispatch_time_days <= 2:
            return "express"
        elif dispatch_time_days <= 5:
            return "fast"
        elif dispatch_time_days <= 10:
            return "standard"
        else:
            return "slow"

    def _publish_listing_created(self, seller_id, item, tier):
        category, category_group = self._get_category(item["product_id"])
        price = float(item["price"])
        seller_state = self.seller_states.get(str(seller_id), "SP")
        seller_region = self.seller_regions.get(str(seller_id), "southeast")
        payload = {
            "listing_id": f"LST-{item['order_id']}-{item['product_id'][:8]}",
            "seller_id": str(seller_id),
            "seller_tier": tier,
            "seller_state": seller_state,
            "seller_region": seller_region,
            "product_id": str(item["product_id"]),
            "category": category,
            "category_group": category_group,
            "price": price,
            "freight_value": float(item.get("freight_value", 0.0)),
            "currency": "BRL",
            "stock_quantity": random.randint(10, 200),
            "region": f"BR-{seller_state}",
        }
        event = self.build_envelope(
            event_type="listing.created",
            payload=payload,
            correlation_id=f"LST-{item['order_id']}-{item['product_id'][:8]}",
        )
        self.publish(TOPIC, event, key=str(seller_id))

    def _publish_order_dispatched(self, seller_id, item, tier):
        dispatch_time_mins = self._get_dispatch_time(tier)
        dispatch_time_days = round(dispatch_time_mins / 1440.0, 2)
        sla_threshold = SLA_THRESHOLDS.get(tier, SLA_THRESHOLDS["new"])
        is_breached = dispatch_time_mins > sla_threshold
        dispatch_speed_bucket = self._get_dispatch_speed_bucket(dispatch_time_days)
        category, category_group = self._get_category(item["product_id"])
        price = float(item["price"])
        freight_value = float(item.get("freight_value", 0.0))
        seller_state = self.seller_states.get(str(seller_id), "SP")
        seller_region = self.seller_regions.get(str(seller_id), "southeast")

        payload = {
            "order_id": str(item["order_id"]),
            "seller_id": str(seller_id),
            "seller_tier": tier,
            "seller_state": seller_state,
            "seller_region": seller_region,
            "listing_id": f"LST-{item['order_id']}-{item['product_id'][:8]}",
            "product_id": str(item["product_id"]),
            "category": category,
            "category_group": category_group,
            "price": price,
            "freight_value": freight_value,
            "dispatch_time_mins": dispatch_time_mins,
            "dispatch_time_days": dispatch_time_days,
            "sla_threshold_mins": sla_threshold,
            "is_sla_breached": is_breached,
            "dispatch_speed_bucket": dispatch_speed_bucket,
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
        category, category_group = self._get_category(item["product_id"])
        seller_state = self.seller_states.get(str(seller_id), "SP")
        seller_region = self.seller_regions.get(str(seller_id), "southeast")
        payload = {
            "listing_id": f"LST-{item['order_id']}-{item['product_id'][:8]}",
            "seller_id": str(seller_id),
            "seller_tier": tier,
            "seller_state": seller_state,
            "seller_region": seller_region,
            "product_id": str(item["product_id"]),
            "category": category,
            "category_group": category_group,
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
