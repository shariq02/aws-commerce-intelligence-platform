import time
import logging
import pandas as pd
from base_generator import BaseGenerator

logger = logging.getLogger(__name__)

TOPIC = "ecommerce.events"

CUSTOMER_SEGMENT_BINS = [0, 0.2, 0.5, 1.0]
CUSTOMER_SEGMENT_LABELS = ["premium", "standard", "new"]


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
            self.customers_df[["customer_id", "customer_city", "customer_state"]],
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
        self.orders_df = self.orders_df.merge(payment_totals, on="order_id", how="left")
        self.orders_df = self.orders_df.merge(payment_methods, on="order_id", how="left")
        self.orders_df["total_amount"] = self.orders_df["total_amount"].fillna(0.0)
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

    def _publish_order_placed(self, order, items):
        payload = {
            "order_id": str(order["order_id"]),
            "customer_id": str(order["customer_id"]),
            "customer_segment": str(order["customer_segment"]),
            "region": f"{order['customer_city']}-{order['customer_state']}",
            "items": items,
            "total_amount": float(order["total_amount"]),
            "payment_method": str(order.get("payment_type", "unknown")),
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
