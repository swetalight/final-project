#!/usr/bin/env python3
"""Запись отфильтрованных данных в Elasticsearch"""

from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ElasticsearchConsumer:
    def __init__(self, config_path="config.json"):
        # Полный путь к config.json
        if not os.path.exists(config_path):
            config_path = "/Users/svetlanaolefirenko/yapracticum/final_projects/config.json"
        
        with open(config_path, "r") as f:
            self.config = json.load(f)

        # =====================================================
        # НАСТРОЙКИ ДЛЯ CONFLUENT_KAFKA
        # =====================================================
        consumer_config = {
            "bootstrap.servers": self.config["kafka"]["bootstrap_servers"],
            "group.id": "es-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": self.config["kafka"]["users"]["analytics"]["username"],
            "sasl.password": self.config["kafka"]["users"]["analytics"]["password"],
            "ssl.ca.location": self.config["kafka"]["ssl"]["truststore"],
            "ssl.certificate.location": self.config["kafka"]["ssl"]["keystore"],
            "ssl.key.location": self.config["kafka"]["ssl"]["key"],
        }

        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe(["filtered-products"])

        # Подключение к Elasticsearch
        self.es = Elasticsearch([self.config["elasticsearch"]["host"]], verify_certs=False)
        logger.info("✅ Подключение к Kafka и Elasticsearch")

    def process_messages(self):
        logger.info("🚀 Запуск Elasticsearch консьюмера...")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"❌ Ошибка: {msg.error()}")
                        break

                try:
                    product = json.loads(msg.value().decode("utf-8"))
                    self.es.index(index="products", id=product["product_id"], body=product)
                    logger.info(f"✅ Индексирован: {product['name']}")
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки: {e}")

        except KeyboardInterrupt:
            logger.info("⏹ Остановка консьюмера...")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = ElasticsearchConsumer()
    consumer.process_messages()