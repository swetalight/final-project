#!/usr/bin/env python3
"""SHOP API — эмуляция отправки товаров в Kafka"""

import json
import time
import os
from confluent_kafka import Producer
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ShopAPI:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)

        kafka_config = {
    "bootstrap.servers": self.config["kafka"]["bootstrap_servers"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": self.config["kafka"]["users"]["shop"]["username"],
    "sasl.password": self.config["kafka"]["users"]["shop"]["password"],
    "ssl.ca.location": self.config["kafka"]["ssl"]["ca"],
    "ssl.certificate.location": self.config["kafka"]["ssl"]["certificate"],
    "ssl.key.location": self.config["kafka"]["ssl"]["key"],
    "acks": "all",
    "retries": 3,
     }

        self.producer = Producer(kafka_config)
        self.topic = self.config["kafka"]["topics"]["products"]
        logger.info(f"✅ Подключение к Kafka: {self.config['kafka']['bootstrap_servers']}")

    def send_products(self, products_file="data/products.json", delay=0.5):
        if not os.path.exists(products_file):
            logger.error(f"❌ Файл {products_file} не найден")
            return

        with open(products_file, "r", encoding="utf-8") as f:
            products = json.load(f)

        logger.info(f"📦 Отправка {len(products)} товаров...")

        for idx, product in enumerate(products, 1):
            message = json.dumps(product, ensure_ascii=False)
            self.producer.produce(
                topic=self.topic,
                key=product["product_id"],
                value=message,
                callback=self._delivery_report,
            )
            self.producer.poll(0)
            logger.info(f"[{idx}/{len(products)}] {product['name']}")
            time.sleep(delay)

        self.producer.flush()
        logger.info("✅ Все товары отправлены")

    def _delivery_report(self, err, msg):
        if err:
            logger.error(f"❌ Ошибка: {err}")
        else:
            logger.info(f"✅ Доставлено: {msg.topic()} [p{msg.partition()}] o{msg.offset()}")


if __name__ == "__main__":
    shop = ShopAPI()
    shop.send_products()