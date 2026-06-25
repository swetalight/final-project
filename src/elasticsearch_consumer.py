#!/usr/bin/env python3
"""
Elasticsearch Consumer — запись отфильтрованных данных в Elasticsearch

Читает данные из топика 'filtered-products' и индексирует их в Elasticsearch.
"""

import json
import os
import sys
import logging
from dotenv import load_dotenv
from confluent_kafka import Consumer
from confluent_kafka import KafkaException
from elasticsearch import Elasticsearch

# Загружаем .env
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ElasticsearchConsumer:
    def __init__(self, config_path="config.json"):
        try:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"❌ Файл {config_path} не найден")

            with open(config_path, "r", encoding="utf-8") as f:
                config_template = json.load(f)

            self.config = self._substitute_env_vars(config_template)
            self._init_consumer()
            self._init_elasticsearch()
            logger.info("✅ Elasticsearch Consumer инициализирован успешно")

        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            sys.exit(1)

    def _substitute_env_vars(self, config):
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            env_var = config[2:-1]
            return os.getenv(env_var, config)
        else:
            return config

    def _init_consumer(self):
        try:
            self.consumer = Consumer({
                "bootstrap.servers": self.config["kafka"]["bootstrap_servers"],
                "group.id": "es-consumer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "security.protocol": self.config["kafka"]["security_protocol"],
                "sasl.mechanism": self.config["kafka"]["sasl_mechanism"],
                "sasl.username": self.config["kafka"]["users"]["analytics"]["username"],
                "sasl.password": self.config["kafka"]["users"]["analytics"]["password"],
                "ssl.ca.location": self.config["kafka"]["ssl"]["ca"],
                "ssl.certificate.location": self.config["kafka"]["ssl"]["certificate"],
                "ssl.key.location": self.config["kafka"]["ssl"]["key"],
            })
            self.consumer.subscribe(["filtered-products"])
            logger.info("✅ Подключение к Kafka (Consumer)")
        except KafkaException as e:
            logger.error(f"❌ Ошибка подключения к Kafka: {e}")
            raise

    def _init_elasticsearch(self):
        try:
            self.es = Elasticsearch(
                [self.config["elasticsearch"]["host"]],
                verify_certs=False,  # Только для разработки
            )
            if self.es.ping():
                logger.info("✅ Подключение к Elasticsearch")
            else:
                logger.warning("⚠️ Elasticsearch не отвечает")
                self.es = None
        except Exception as e:
            logger.warning(f"⚠️ Ошибка подключения к Elasticsearch: {e}")
            self.es = None

    def process_messages(self):
        """Обработка сообщений из Kafka и запись в Elasticsearch"""
        logger.info("🚀 Запуск Elasticsearch консьюмера...")

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"❌ Ошибка: {msg.error()}")
                    continue

                try:
                    product = json.loads(msg.value().decode("utf-8"))
                    if self.es:
                        self.es.index(
                            index=self.config["elasticsearch"]["index"],
                            id=product.get("product_id"),
                            body=product
                        )
                        logger.info(f"✅ Индексирован: {product.get('name', 'unknown')}")
                    else:
                        logger.warning(f"⚠️ Elasticsearch недоступен: {product.get('name')}")
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки: {e}")

        except KeyboardInterrupt:
            logger.info("⏹ Остановка консьюмера...")
        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = ElasticsearchConsumer()
    consumer.process_messages()