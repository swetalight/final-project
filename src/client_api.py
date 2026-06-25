#!/usr/bin/env python3
"""
CLIENT API — эмуляция запросов клиентов

Команды:
- search <имя> — поиск товара по имени
- rec <user_id> — получение рекомендаций
"""

import json
import os
import uuid
import sys
import logging
import warnings
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka import KafkaException
from elasticsearch import Elasticsearch

# Загружаем .env
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ClientAPI:
    def __init__(self, config_path="config.json"):
        try:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"❌ Файл {config_path} не найден")

            with open(config_path, "r", encoding="utf-8") as f:
                config_template = json.load(f)

            self.config = self._substitute_env_vars(config_template)
            self._init_producer()
            self._init_elasticsearch()
            logger.info("✅ CLIENT API инициализирован успешно")

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

    def _init_producer(self):
        try:
            kafka_config = {
                "bootstrap.servers": self.config["kafka"]["bootstrap_servers"],
                "security.protocol": self.config["kafka"]["security_protocol"],
                "sasl.mechanism": self.config["kafka"]["sasl_mechanism"],
                "sasl.username": self.config["kafka"]["users"]["client"]["username"],
                "sasl.password": self.config["kafka"]["users"]["client"]["password"],
                "ssl.ca.location": self.config["kafka"]["ssl"]["ca"],
                "ssl.certificate.location": self.config["kafka"]["ssl"]["certificate"],
                "ssl.key.location": self.config["kafka"]["ssl"]["key"],
                "acks": "all",
                "retries": 3,
            }
            self.producer = Producer(kafka_config)
            self.client_topic = self.config["kafka"]["topics"]["client_requests"]
            logger.info("✅ Подключение к Kafka")
        except KafkaException as e:
            logger.error(f"❌ Ошибка подключения к Kafka: {e}")
            raise

    def _init_elasticsearch(self):
        """Безопасное подключение к Elasticsearch с проверкой сертификатов"""
        try:
            host = self.config["elasticsearch"]["host"]
            use_ssl = os.getenv("ELASTICSEARCH_USE_SSL", "true").lower() == "true"
            ca_certs = os.getenv("ELASTICSEARCH_CA_CERTS", None)
            username = os.getenv("ELASTICSEARCH_USERNAME", None)
            password = os.getenv("ELASTICSEARCH_PASSWORD", None)

            if use_ssl and ca_certs:
                # ✅ БЕЗОПАСНОЕ ПОДКЛЮЧЕНИЕ
                self.es = Elasticsearch(
                    [host],
                    verify_certs=True,
                    ca_certs=ca_certs,
                    basic_auth=(username, password) if username and password else None,
                )
                logger.info("✅ Безопасное подключение к Elasticsearch (SSL)")

            elif use_ssl and not ca_certs:
                # ⚠️ ТОЛЬКО ДЛЯ РАЗРАБОТКИ
                warnings.warn(
                    "⚠️ SSL используется без CA-сертификата! verify_certs=False (ТОЛЬКО ДЛЯ РАЗРАБОТКИ)",
                    RuntimeWarning
                )
                self.es = Elasticsearch([host], verify_certs=False)
                logger.warning("⚠️ Подключение к Elasticsearch без проверки сертификата (небезопасно!)")

            else:
                # HTTP (без SSL)
                self.es = Elasticsearch([host], verify_certs=False)
                logger.info("✅ Подключение к Elasticsearch (HTTP)")

            if self.es.ping():
                logger.info("✅ Elasticsearch доступен")
            else:
                logger.warning("⚠️ Elasticsearch не отвечает")

        except Exception as e:
            logger.warning(f"⚠️ Ошибка подключения к Elasticsearch: {e}")
            self.es = None

    def search_product(self, query):
        try:
            request = {
                "request_id": str(uuid.uuid4()),
                "type": "search",
                "query": query,
                "timestamp": "2026-06-25T12:00:00Z"
            }
            self.producer.produce(topic=self.client_topic, value=json.dumps(request))
            self.producer.flush()
            logger.info(f"🔍 Поиск: '{query}'")

            if self.es:
                result = self.es.search(
                    index="products",
                    body={"query": {"match": {"name": query}}, "size": 5}
                )
                for hit in result["hits"]["hits"]:
                    p = hit["_source"]
                    logger.info(f"  📦 {p['name']} — {p['price']['amount']} {p['price']['currency']}")
                return result
            else:
                logger.warning("⚠️ Elasticsearch недоступен")
                return None
        except Exception as e:
            logger.error(f"❌ Ошибка поиска: {e}")
            return None

    def get_recommendations(self, user_id):
        try:
            request = {
                "request_id": str(uuid.uuid4()),
                "type": "recommendation",
                "user_id": user_id,
                "timestamp": "2026-06-25T12:00:00Z"
            }
            self.producer.produce(topic=self.client_topic, value=json.dumps(request))
            self.producer.flush()
            logger.info(f"🎯 Рекомендации для пользователя: {user_id}")

            recommendations = [
                {"product_id": "12345", "name": "Умные часы XYZ", "score": 0.95},
                {"product_id": "12346", "name": "Смартфон ABC Pro", "score": 0.87},
                {"product_id": "12347", "name": "Наушники XYZ Air", "score": 0.82},
            ]
            for rec in recommendations:
                logger.info(f"  ⭐ {rec['name']} (score: {rec['score']})")
            return recommendations
        except Exception as e:
            logger.error(f"❌ Ошибка получения рекомендаций: {e}")
            return []


if __name__ == "__main__":
    client = ClientAPI()
    while True:
        cmd = input("\n📌 Команда (search <имя> / rec <user_id> / exit): ")
        if cmd == "exit":
            break
        elif cmd.startswith("search "):
            client.search_product(cmd[7:])
        elif cmd.startswith("rec "):
            client.get_recommendations(cmd[4:])
        else:
            print("❌ Неизвестная команда")