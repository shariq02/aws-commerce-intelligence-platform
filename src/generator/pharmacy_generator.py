import time
import logging
import random
import hashlib
import pandas as pd
from base_generator import BaseGenerator

logger = logging.getLogger(__name__)

TOPIC = "pharmacy.events"

ATC_CATEGORIES = {
    "M01AB": {"name": "anti_inflammatory_acetic_acid",   "category_group": "anti_inflammatory", "is_prescription": True,  "drug_class": "NSAID"},
    "M01AE": {"name": "anti_inflammatory_propionic_acid","category_group": "anti_inflammatory", "is_prescription": False, "drug_class": "NSAID"},
    "N02BA": {"name": "analgesic_salicylic_acid",         "category_group": "analgesic",         "is_prescription": False, "drug_class": "salicylate"},
    "N02BE": {"name": "analgesic_anilide",                "category_group": "analgesic",         "is_prescription": False, "drug_class": "anilide"},
    "N05B":  {"name": "anxiolytic",                       "category_group": "psychoactive",      "is_prescription": True,  "drug_class": "benzodiazepine"},
    "N05C":  {"name": "hypnotic_sedative",                "category_group": "psychoactive",      "is_prescription": True,  "drug_class": "sedative"},
    "R03":   {"name": "respiratory_obstructive",          "category_group": "respiratory",       "is_prescription": True,  "drug_class": "bronchodilator"},
    "R06":   {"name": "antihistamine",                    "category_group": "respiratory",       "is_prescription": False, "drug_class": "antihistamine"},
}

FILL_TIME_PARAMS = {
    True:  {"mean": 25, "std": 8},
    False: {"mean": 10, "std": 3},
}

REGIONS = [
    "DE-Hamburg", "DE-Berlin", "DE-Munich", "DE-Frankfurt",
    "DE-Cologne", "DE-Stuttgart", "DE-Dusseldorf", "DE-Leipzig",
]

INITIAL_STOCK = 500
REORDER_THRESHOLD = int(INITIAL_STOCK * 0.1)

# Fix applied June 2026:
# prescription.filled and inventory.updated payloads were minimal --
# missing product_id, category, atc_code, drug_class, quantity,
# is_prescription, stock_level, reorder_threshold fields.
# Gold notebook 14 was reading these from payload and getting nulls
# for streaming events, causing 3,515 null stock_level rows.
# All fields now included in every event type payload.


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

    def _build_full_payload(self, drug_code, quantity, stock_level, fill_time_mins, row):
        """Build complete payload with all fields needed by Gold notebook 14."""
        info = ATC_CATEGORIES[drug_code]
        reorder_threshold = REORDER_THRESHOLD
        days_of_supply = max(0, round(stock_level / max(quantity, 1), 1))

        if stock_level <= reorder_threshold * 0.5:
            stock_alert_level = "critical"
        elif stock_level <= reorder_threshold:
            stock_alert_level = "high"
        elif stock_level <= reorder_threshold * 2:
            stock_alert_level = "medium"
        else:
            stock_alert_level = "normal"

        hour = getattr(row, "Hour", 0) if hasattr(row, "Hour") else 0
        if isinstance(hour, float):
            hour = int(hour)

        if 6 <= hour <= 11:
            time_of_day = "morning"
        elif 12 <= hour <= 17:
            time_of_day = "afternoon"
        elif 18 <= hour <= 22:
            time_of_day = "evening"
        else:
            time_of_day = "night"

        is_peak_hour = (10 <= hour <= 12) or (16 <= hour <= 19)
        weekday = getattr(row, "weekday_name", "Monday") if hasattr(row, "weekday_name") else "Monday"
        is_weekend = str(weekday) in ("Saturday", "Sunday")

        return {
            "product_id": self._get_product_id(drug_code),
            "category": info["name"],
            "category_group": info["category_group"],
            "atc_code": drug_code,
            "drug_class": info["drug_class"],
            "quantity": float(quantity),
            "is_prescription": info["is_prescription"],
            "stock_level": stock_level,
            "reorder_threshold": reorder_threshold,
            "days_of_supply": days_of_supply,
            "stock_alert_level": stock_alert_level,
            "fill_time_mins": fill_time_mins,
            "time_of_day": time_of_day,
            "is_peak_hour": is_peak_hour,
            "is_weekend": is_weekend,
            "hour": hour,
            "weekday": str(weekday),
        }

    def _publish_prescription_submitted(self, row, drug_code, quantity, correlation_id):
        fill_time = self._get_fill_time(ATC_CATEGORIES[drug_code]["is_prescription"])
        # Fix: include full payload so Gold notebook 14 has all fields
        payload = self._build_full_payload(
            drug_code, quantity, self.stock_levels[drug_code], fill_time, row
        )
        payload["prescription_id"] = correlation_id
        payload["region"] = self._get_region(row.name)
        payload["unit_price"] = round(random.uniform(5.0, 45.0), 2)

        event = self.build_envelope(
            event_type="prescription.submitted",
            payload=payload,
            correlation_id=correlation_id,
        )
        self.publish(TOPIC, event, key=self._get_product_id(drug_code))

    def _publish_prescription_filled(self, row, drug_code, quantity, correlation_id):
        # Fix: previously minimal payload missing all critical fields
        # Now includes complete payload identical to prescription.submitted
        self.stock_levels[drug_code] = max(
            0, self.stock_levels[drug_code] - 1
        )
        fill_time = self._get_fill_time(ATC_CATEGORIES[drug_code]["is_prescription"])
        payload = self._build_full_payload(
            drug_code, quantity, self.stock_levels[drug_code], fill_time, row
        )
        payload["prescription_id"] = correlation_id
        payload["is_substituted"] = False

        event = self.build_envelope(
            event_type="prescription.filled",
            payload=payload,
            correlation_id=correlation_id,
        )
        self.publish(TOPIC, event, key=self._get_product_id(drug_code))

    def _publish_inventory_update(self, drug_code, row):
        # Fix: previously minimal payload missing critical fields
        stock = self.stock_levels[drug_code]
        payload = self._build_full_payload(
            drug_code, 1, stock, 0, row
        )
        payload["warehouse_id"] = "WH-Hamburg-01"
        payload["update_reason"] = "sale"

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
                self._publish_prescription_filled(
                    row, drug_code, quantity, correlation_id
                )
                counter += 1
                if counter % self.inventory_publish_interval == 0:
                    self._publish_inventory_update(drug_code, row)
                time.sleep(delay)

            if (idx + 1) % self.CHECKPOINT_INTERVAL == 0:
                self.save_checkpoint(idx + 1)
                logger.info(f"Checkpoint saved: row {idx + 1}/{len(self.sales_df)}")

        self.flush()
        self.save_checkpoint(len(self.sales_df), completed=True)
        logger.info("Pharmacy generator completed.")
