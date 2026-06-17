import time
import logging
import random
import hashlib
import pandas as pd
from datetime import datetime, timezone, timedelta
from base_generator import BaseGenerator

logger = logging.getLogger(__name__)

TOPIC = "pharmacy.events"

ATC_CATEGORIES = {
    "M01AB": {"name": "Anti-inflammatory", "is_prescription": True},
    "M01AE": {"name": "Anti-inflammatory-OTC", "is_prescription": False},
    "N02BA": {"name": "Analgesic", "is_prescription": False},
    "N02BE": {"name": "Analgesic-OTC", "is_prescription": False},
    "N05B": {"name": "Anxiolytic", "is_prescription": True},
    "N05C": {"name": "Hypnotic", "is_prescription": True},
    "R03": {"name": "Respiratory", "is_prescription": True},
    "R06": {"name": "Antihistamine", "is_prescription": False},
}

FILL_TIME_PARAMS = {
    True: {"mean": 25, "std": 8},
    False: {"mean": 10, "std": 3},
}

REGIONS = [
    "DE-Hamburg", "DE-Berlin", "DE-Munich", "DE-Frankfurt",
    "DE-Cologne", "DE-Stuttgart", "DE-Dusseldorf", "DE-Leipzig",
]

INITIAL_STOCK = 500


class PharmacyGenerator(BaseGenerator):

    def __init__(self, data_path, event_rate=10, run_id=None):
        super().__init__(
            domain="pharmacy",
            source_system="pharmacy-simulator",
            event_rate=event_rate,
            run_id=run_id,
        )
        self.data_path = data_path
        self.sales_df = None
        self.stock_levels = {}
        self.inventory_publish_interval = 50

    def load_data(self):
        logger.info("Loading Pharma Sales dataset...")
        self.sales_df = pd.read_csv(f"{self.data_path}/saleshourly.csv")
        self.sales_df["datum"] = pd.to_datetime(self.sales_df["datum"])
        for drug_code in ATC_CATEGORIES:
            self.stock_levels[drug_code] = INITIAL_STOCK
        logger.info(f"Loaded {len(self.sales_df)} pharmacy records successfully.")

    def _get_product_id(self, drug_code):
        return hashlib.md5(drug_code.encode()).hexdigest()[:12]

    def _get_region(self, index):
        return REGIONS[index % len(REGIONS)]

    def _get_fill_time(self, is_prescription):
        params = FILL_TIME_PARAMS[is_prescription]
        return max(5, int(random.gauss(params["mean"], params["std"])))

    def _publish_prescription_submitted(self, row, drug_code, quantity, correlation_id):
        category_info = ATC_CATEGORIES[drug_code]
        payload = {
            "prescription_id": correlation_id,
            "product_id": self._get_product_id(drug_code),
            "product_name": category_info["name"],
            "category": drug_code,
            "is_prescription": category_info["is_prescription"],
            "quantity": quantity,
            "unit_price": round(random.uniform(5.0, 45.0), 2),
            "region": self._get_region(row.name),
        }
        event = self.build_envelope(
            event_type="prescription.submitted",
            payload=payload,
            correlation_id=correlation_id,
        )
        self.publish(TOPIC, event, key=self._get_product_id(drug_code))

    def _publish_prescription_filled(self, row, drug_code, correlation_id):
        category_info = ATC_CATEGORIES[drug_code]
        fill_time = self._get_fill_time(category_info["is_prescription"])
        self.stock_levels[drug_code] = max(
            0, self.stock_levels[drug_code] - 1
        )
        payload = {
            "prescription_id": correlation_id,
            "fill_time_mins": fill_time,
            "stock_level_post": self.stock_levels[drug_code],
            "is_substituted": False,
        }
        event = self.build_envelope(
            event_type="prescription.filled",
            payload=payload,
            correlation_id=correlation_id,
        )
        self.publish(TOPIC, event, key=self._get_product_id(drug_code))

    def _publish_inventory_update(self, drug_code):
        stock = self.stock_levels[drug_code]
        reorder_threshold = int(INITIAL_STOCK * 0.1)
        days_of_supply = max(0, int(stock / 10))
        payload = {
            "product_id": self._get_product_id(drug_code),
            "warehouse_id": "WH-Hamburg-01",
            "stock_level": stock,
            "reorder_threshold": reorder_threshold,
            "days_of_supply": days_of_supply,
            "update_reason": "sale",
        }
        event = self.build_envelope(
            event_type="inventory.updated",
            payload=payload,
        )
        self.publish(TOPIC, event, key=self._get_product_id(drug_code))

    def generate(self):
        self.load_data()

        start_row, completed = self.load_checkpoint()
        if completed:
            return

        logger.info(f"Starting pharmacy generator at {self.event_rate} events/sec...")
        if start_row > 0:
            logger.info(f"Resuming from row {start_row} of {len(self.sales_df)}")

        delay = 1.0 / self.event_rate
        counter = 0

        for idx, row in self.sales_df.iterrows():
            if idx < start_row:
                continue

            for drug_code in ATC_CATEGORIES:
                quantity = row.get(drug_code, 0)
                if pd.isna(quantity) or quantity <= 0:
                    continue
                quantity = int(round(quantity))
                correlation_id = f"RX-{idx}-{drug_code}"
                self._publish_prescription_submitted(
                    row, drug_code, quantity, correlation_id
                )
                self._publish_prescription_filled(row, drug_code, correlation_id)
                counter += 1
                if counter % self.inventory_publish_interval == 0:
                    self._publish_inventory_update(drug_code)
                time.sleep(delay)

            if (idx + 1) % self.CHECKPOINT_INTERVAL == 0:
                self.save_checkpoint(idx + 1)
                logger.info(f"Checkpoint saved: row {idx + 1}/{len(self.sales_df)}")

        self.flush()
        self.save_checkpoint(len(self.sales_df), completed=True)
        logger.info("Pharmacy generator completed.")
