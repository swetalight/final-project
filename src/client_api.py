#!/usr/bin/env python3
import json
import uuid
from confluent_kafka import Producer
from elasticsearch import Elasticsearch
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class ClientAPI:
    def __init__(self, config_path="config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)

        # =====================================================
        # Исправленная конфигурация Kafka
        # =====================================================
        kafka_config = {
            "bootstrap.servers": self.config["kafka"]["bootstrap_servers"],
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": self.config["kafka"]["users"]["client"]["username"],
            "sasl.password": self.config["kafka"]["users"]["client"]["password"],
            # ПРАВИЛЬНЫЕ SSL настройки (PEM файлы)
            "ssl.ca.location": self.config["kafka"]["ssl"]["truststore"],
            "ssl.certificate.location": self.config["kafka"]["ssl"]["keystore"],
            "ssl.key.location": self.config["kafka"]["ssl"]["key"],
            "acks": "all",
            "retries": 3,
        }

        self.producer = Producer(kafka_config)
        self.client_topic = self.config["kafka"]["topics"]["client_requests"]

        # Подключение к Elasticsearch
        self.es = Elasticsearch([self.config["elasticsearch"]["host"]], verify_certs=False)
        logger.info("✅ Подключение к Kafka и Elasticsearch")

    def search_product(self, query):
        request = {"request_id": str(uuid.uuid4()), "type": "search", "query": query}
        self.producer.produce(topic=self.client_topic, value=json.dumps(request))
        self.producer.flush()
        logger.info(f"🔍 Поиск: '{query}'")

        result = self.es.search(index="products", body={"query": {"match": {"name": query}}, "size": 5})
        for hit in result["hits"]["hits"]:
            p = hit["_source"]
            logger.info(f"  📦 {p['name']} — {p['price']['amount']} {p['price']['currency']}")
        return result

    def get_recommendations(self, user_id):
        request = {"request_id": str(uuid.uuid4()), "type": "recommendation", "user_id": user_id}
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