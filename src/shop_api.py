#!/usr/bin/env python3
"""
SHOP API — эмуляция отправки товаров в Kafka

Магазины отправляют информацию о товарах в формате JSON.
Данные читаются из файла data/products.json и отправляются в Kafka-топик 'products'.
"""

import json
import os
import time
import sys
import logging
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka import KafkaException

# Загружаем переменные окружения из .env файла
# Ищет .env в текущей директории и выше
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


class ShopAPI:
    """SHOP API — эмуляция отправки товаров в Kafka"""

    def __init__(self, config_path="config.json"):
        """
        Инициализация SHOP API

        Args:
            config_path: Путь к файлу конфигурации
        """
        try:
            # Проверка существования файла
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"❌ Файл {config_path} не найден")

            # Читаем config.json
            with open(config_path, "r", encoding="utf-8") as f:
                config_template = json.load(f)

            # Подставляем переменные окружения
            self.config = self._substitute_env_vars(config_template)

            # Инициализируем продюсера
            self._init_producer()

            logger.info("✅ SHOP API инициализирован успешно")

        except FileNotFoundError as e:
            logger.error(e)
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга {config_path}: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            sys.exit(1)

    def _substitute_env_vars(self, config):
        """
        Рекурсивно подставляет переменные окружения в конфиг

        Args:
            config: Словарь или строка конфигурации

        Returns:
            Конфигурация с подставленными переменными
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            env_var = config[2:-1]
            value = os.getenv(env_var)
            if value is None:
                logger.warning(f"⚠️ Переменная {env_var} не найдена, используется значение по умолчанию: {config}")
                return config
            return value
        else:
            return config

    def _init_producer(self):
        """
        Инициализация Kafka продюсера
        """
        try:
            kafka_config = {
                "bootstrap.servers": self.config["kafka"]["bootstrap_servers"],
                "security.protocol": self.config["kafka"]["security_protocol"],
                "sasl.mechanism": self.config["kafka"]["sasl_mechanism"],
                "sasl.username": self.config["kafka"]["users"]["shop"]["username"],
                "sasl.password": self.config["kafka"]["users"]["shop"]["password"],
                "ssl.ca.location": self.config["kafka"]["ssl"]["ca"],
                "ssl.certificate.location": self.config["kafka"]["ssl"]["certificate"],
                "ssl.key.location": self.config["kafka"]["ssl"]["key"],
                "acks": "all",  # Подтверждение от всех реплик
                "retries": 3,   # Повтор при ошибках
                "batch.size": 16384,
                "linger.ms": 5,
            }

            # Для тестирования можно отключить проверку сертификата
            # В production используйте настоящие сертификаты
            kafka_config["enable.ssl.certificate.verification"] = False

            self.producer = Producer(kafka_config)
            self.topic = self.config["kafka"]["topics"]["products"]

            logger.info(f"✅ Подключение к Kafka: {self.config['kafka']['bootstrap_servers']}")
            logger.info(f"📌 Топик: {self.topic}")
            logger.info(f"👤 Пользователь: {self.config['kafka']['users']['shop']['username']}")

        except KafkaException as e:
            logger.error(f"❌ Ошибка подключения к Kafka: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации продюсера: {e}")
            raise

    def send_products(self, products_file="data/products.json", delay=0.5):
        """
        Отправка товаров в Kafka

        Args:
            products_file: Путь к файлу с товарами
            delay: Задержка между отправками (сек)
        """
        try:
            # Проверка существования файла
            if not os.path.exists(products_file):
                logger.error(f"❌ Файл {products_file} не найден")
                return

            # Чтение товаров из файла
            with open(products_file, "r", encoding="utf-8") as f:
                products = json.load(f)

            logger.info(f"📦 Начинаем отправку {len(products)} товаров...")
            logger.info("=" * 60)

            # Отправка каждого товара
            for idx, product in enumerate(products, 1):
                # Сериализация в JSON
                message = json.dumps(product, ensure_ascii=False)

                # Отправка в Kafka
                self.producer.produce(
                    topic=self.topic,
                    key=product.get("product_id", str(idx)),
                    value=message,
                    callback=self._delivery_report,
                )

                # Обработка событий
                self.producer.poll(0)

                logger.info(f"[{idx}/{len(products)}] 📤 {product.get('name', 'unknown')}")

                # Задержка для имитации реальной отправки
                time.sleep(delay)

            # Ожидание завершения отправки
            self.producer.flush()
            logger.info("=" * 60)
            logger.info("✅ Все товары успешно отправлены!")

        except json.JSONDecodeError as e:
            logger.error(f"❌ Ошибка парсинга {products_file}: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки: {e}")

    def send_single_product(self, product):
        """
        Отправка одного товара

        Args:
            product: Словарь с данными товара
        """
        try:
            message = json.dumps(product, ensure_ascii=False)

            self.producer.produce(
                topic=self.topic,
                key=product.get("product_id"),
                value=message,
                callback=self._delivery_report,
            )
            self.producer.flush()

            logger.info(f"✅ Товар {product.get('name', 'unknown')} отправлен")

        except Exception as e:
            logger.error(f"❌ Ошибка отправки: {e}")

    def _delivery_report(self, err, msg):
        """
        Callback при доставке сообщения

        Args:
            err: Ошибка (если есть)
            msg: Сообщение
        """
        if err is not None:
            logger.error(f"❌ Ошибка доставки: {err}")
        else:
            logger.info(
                f"✅ Доставлено: topic={msg.topic()}, "
                f"partition={msg.partition()}, offset={msg.offset()}"
            )


def main():
    """Точка входа"""
    logger.info("🚀 Запуск SHOP API...")

    # Создание экземпляра SHOP API
    shop = ShopAPI()

    # Отправка товаров
    shop.send_products()

    logger.info("🏁 SHOP API завершил работу")


if __name__ == "__main__":
    main()