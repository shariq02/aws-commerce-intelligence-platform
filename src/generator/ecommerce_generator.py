import time
import logging
import pandas as pd
from base_generator import BaseGenerator

logger = logging.getLogger(__name__)

TOPIC = "ecommerce.events"

CUSTOMER_SEGMENT_BINS = [0, 0.2, 0.5, 1.0]
CUSTOMER_SEGMENT_LABELS = ["premium", "standard", "new"]

REGION_MAP = {
    "SP": "southeast", "RJ": "southeast", "MG": "southeast", "ES": "southeast",
    "RS": "south",     "PR": "south",     "SC": "south",
    "BA": "northeast", "PE": "northeast", "CE": "northeast", "MA": "northeast",
    "PA": "northeast", "PB": "northeast", "RN": "northeast", "AL": "northeast",
    "SE": "northeast", "PI": "northeast",
    "DF": "center_west", "GO": "center_west", "MT": "center_west", "MS": "center_west",
    "AM": "north", "PA": "north", "RO": "north", "AC": "north",
    "RR": "north", "AP": "north", "TO": "north",
}

# Fix applied June 2026:
# order_status was missing from payload -- Gold notebook 13 was reading null
# Added order_status, state_region, customer_state, fulfilment fields to order.placed
# These fields are present in Olist dataset and needed for Gold layer joins


class EcommerceGenerator(BaseGenerator):

    def __init__(self, data_path, event_rate=10, run_id=None):
        super().__init__(
            domain="ecommerce",
            source_system="ecommerce-simulator",
            event_rate=event_rate,
            run_id=run_id,
        )
        self.data_path = data_path
        self.orders_df = None
        self.items_df = None
        self.payments_df = None
        self.customers_df = None
        self.products_df = None
        self.reviews_df = None
        self.translations_df = None

    def load_data(self):
        logger.info("Loading Olist e-commerce datasets...")
        self.orders_df = pd.read_csv(f"{self.data_path}/olist_orders_dataset.csv")
        self.items_df = pd.read_csv(f"{self.data_path}/olist_order_items_dataset.csv")
        self.payments_df = pd.read_csv(f"{self.data_path}/olist_order_payments_dataset.csv")
        self.customers_df = pd.read_csv(f"{self.data_path}/olist_customers_dataset.csv")
        self.products_df = pd.read_csv(f"{self.data_path}/olist_products_dataset.csv")
        self.reviews_df = pd.read_csv(f"{self.data_path}/olist_order_reviews_dataset.csv")
        self.translations_df = pd.read_csv(f"{self.data_path}/product_category_name_translation.csv")
        self._enrich_data()
        logger.info(f"Loaded {len(self.orders_df)} orders successfully.")

    def _enrich_data(self):
        self.orders_df = self.orders_df.merge(
            self.customers_df[[
                "customer_id", "customer_unique_id",
                "customer_city", "customer_state"
            ]],
            on="customer_id",
            how="left",
        )
        payment_totals = (
            self.payments_df.groupby("order_id")["payment_value"]
            .sum()
            .reset_index()
            .rename(columns={"payment_value": "total_amount"})
        )
        payment_methods = (
            self.payments_df.groupby("order_id")["payment_type"]
            .first()
            .reset_index()
        )
        max_installments = (
            self.payments_df.groupby("order_id")["payment_installments"]
            .max()
            .reset_index()
            .rename(columns={"payment_installments": "max_installments"})
        )
        self.orders_df = self.orders_df.merge(payment_totals, on="order_id", how="left")
        self.orders_df = self.orders_df.merge(payment_methods, on="order_id", how="left")
        self.orders_df = self.orders_df.merge(max_installments, on="order_id", how="left")
        self.orders_df["total_amount"] = self.orders_df["total_amount"].fillna(0.0)
        self.orders_df["max_installments"] = self.orders_df["max_installments"].fillna(1).astype(int)

        quantiles = self.orders_df["total_amount"].quantile(
            CUSTOMER_SEGMENT_BINS
        ).values
        self.orders_df["customer_segment"] = pd.cut(
            self.orders_df["total_amount"],
            bins=quantiles,
            labels=CUSTOMER_SEGMENT_LABELS,
            include_lowest=True,
        ).astype(str)

        self.products_df = self.products_df.merge(
            self.translations_df,
            on="product_category_name",
            how="left",
        )
        self.products_df["product_category_name_english"] = (
            self.products_df["product_category_name_english"].fillna("other")
        )

        # Merge review scores for fulfilment enrichment
        review_scores = (
            self.reviews_df.groupby("order_id")["review_score"]
            .mean()
            .reset_index()
            .rename(columns={"review_score": "avg_review_score"})
        )
        self.orders_df = self.orders_df.merge(review_scores, on="order_id", how="left")

    def _build_items(self, order_id):
        items = self.items_df[self.items_df["order_id"] == order_id]
        result = []
        for _, item in items.iterrows():
            product = self.products_df[
                self.products_df["product_id"] == item["product_id"]
            ]
            category = (
                product["product_category_name_english"].values[0]
                if len(product) > 0
                else "other"
            )
            result.append({
                "product_id": str(item["product_id"]),
                "category": category,
                "quantity": 1,
                "unit_price": float(item["price"]),
                "freight_value": float(item["freight_value"]),
                "currency": "BRL",
            })
        return result

    def _get_fulfilment_info(self, order):
        """Calculate fulfilment time and bucket from order timestamps."""
        try:
            purchase_time = pd.to_datetime(order["order_purchase_timestamp"])
            delivery_time = pd.to_datetime(order.get("order_delivered_customer_date"))
            if pd.isna(delivery_time):
                return None, None, None, None
            fulfilment_mins = int((delivery_time - purchase_time).total_seconds() / 60)
            fulfilment_days = round(fulfilment_mins / 1440.0, 2)
            if fulfilment_days <= 1:
                bucket = "express"
            elif fulfilment_days <= 5:
                bucket = "standard"
            elif fulfilment_days <= 14:
                bucket = "slow"
            else:
                bucket = "very_slow"
            estimated = pd.to_datetime(order.get("order_estimated_delivery_date"))
            delivery_on_time = (
                delivery_time <= estimated
                if not pd.isna(estimated)
                else None
            )
            return fulfilment_mins, fulfilment_days, bucket, delivery_on_time
        except Exception:
            return None, None, None, None

    def _publish_order_placed(self, order, items):
        state = str(order.get("customer_state", "SP"))
        state_region = REGION_MAP.get(state, "other")
        fulfilment_mins, fulfilment_days, fulfilment_bucket, delivery_on_time = \
            self._get_fulfilment_info(order)

        avg_review = order.get("avg_review_score")
        avg_review = float(avg_review) if not pd.isna(avg_review) else None

        review_sentiment = None
        has_negative_review = False
        if avg_review is not None:
            if avg_review >= 4:
                review_sentiment = "positive"
            elif avg_review >= 3:
                review_sentiment = "neutral"
            else:
                review_sentiment = "negative"
                has_negative_review = True

        # FIX: added order_status, state_region, customer_state, customer_unique_id,
        # fulfilment fields, review fields, is_installment, max_installments
        # Previously missing from payload causing null order_status in Gold layer
        payload = {
            "order_id": str(order["order_id"]),
            "customer_id": str(order["customer_id"]),
            "customer_unique_id": str(order.get("customer_unique_id", "")),
            "customer_segment": str(order["customer_segment"]),
            "region": f"{order['customer_city']}-{state}",
            "state_region": state_region,
            "customer_state": state,
            "items": items,
            "total_amount": float(order["total_amount"]),
            "payment_method": str(order.get("payment_type", "unknown")),
            "is_installment": bool(order.get("max_installments", 1) > 1),
            "max_installments": int(order.get("max_installments", 1)),
            "order_status": str(order.get("order_status", "unknown")),
            "item_count": len(items),
            "is_multi_item": len(items) > 1,
            "is_multi_seller": len(set(i.get("seller_id", "") for i in items)) > 1,
            "fulfilment_time_mins": fulfilment_mins,
            "fulfilment_time_days": fulfilment_days,
            "fulfilment_bucket": fulfilment_bucket,
            "delivery_on_time": delivery_on_time,
            "avg_review_score": avg_review,
            "review_sentiment": review_sentiment,
            "has_negative_review": has_negative_review,
            "channel": "web",
        }
        event = self.build_envelope(
            event_type="order.placed",
            payload=payload,
            correlation_id=str(order["order_id"]),
        )
        self.publish(TOPIC, event, key=str(order["customer_id"]))

    def _publish_order_fulfilled(self, order):
        if pd.isna(order.get("order_delivered_customer_date")):
            return
        purchase_time = pd.to_datetime(order["order_purchase_timestamp"])
        delivery_time = pd.to_datetime(order["order_delivered_customer_date"])
        fulfilment_mins = int(
            (delivery_time - purchase_time).total_seconds() / 60
        )
        payload = {
            "order_id": str(order["order_id"]),
            "fulfilment_time_mins": fulfilment_mins,
            "carrier": "correios",
            "estimated_delivery": str(order.get("order_estimated_delivery_date", "")),
        }
        event = self.build_envelope(
            event_type="order.fulfilled",
            payload=payload,
            correlation_id=str(order["order_id"]),
        )
        self.publish(TOPIC, event, key=str(order["customer_id"]))

    def _publish_order_returned(self, order):
        review = self.reviews_df[self.reviews_df["order_id"] == order["order_id"]]
        if len(review) == 0:
            return
        score = review["review_score"].values[0]
        if score > 2:
            return
        payload = {
            "order_id": str(order["order_id"]),
            "return_reason": "low_review_score",
            "review_score": int(score),
            "refund_amount": float(order["total_amount"]),
        }
        event = self.build_envelope(
            event_type="order.returned",
            payload=payload,
            correlation_id=str(order["order_id"]),
        )
        self.publish(TOPIC, event, key=str(order["customer_id"]))

    def generate(self):
        self.load_data()

        start_row, completed = self.load_checkpoint()
        if completed:
            return

        logger.info(f"Starting e-commerce generator at {self.event_rate} events/sec...")
        if start_row > 0:
            logger.info(f"Resuming from row {start_row} of {len(self.orders_df)}")

        delay = 1.0 / self.event_rate

        for idx, (_, order) in enumerate(self.orders_df.iterrows()):
            if idx < start_row:
                continue

            items = self._build_items(order["order_id"])
            self._publish_order_placed(order, items)
            self._publish_order_fulfilled(order)
            self._publish_order_returned(order)
            time.sleep(delay)

            if (idx + 1) % self.CHECKPOINT_INTERVAL == 0:
                self.save_checkpoint(idx + 1)
                logger.info(f"Checkpoint saved: row {idx + 1}/{len(self.orders_df)}")

        self.flush()
        self.save_checkpoint(len(self.orders_df), completed=True)
        logger.info("E-commerce generator completed.")
