#!/usr/bin/env python3
"""
Faust-процессор для фильтрации запрещённых товаров

Читает товары из топика 'products', проверяет список запрещённых,
разрешённые отправляет в топик 'filtered-products'.
"""

import json
import os
import logging
import faust
from dotenv import load_dotenv

# Загружаем .env
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Product(faust.Record, serializer="json"):
    product_id: str
    name: str
    description: str
    price: dict
    category: str
    brand: str
    stock: dict
    sku: str
    tags: list
    images: list
    specifications: dict
    created_at: str
    updated_at: str
    store_id: str


class FilteredProduct(faust.Record, serializer="json"):
    product_id: str
    name: str
    price: dict
    category: str
    brand: str
    store_id: str


# Создаём Faust-приложение
app = faust.App(
    "marketplace-processor",
    broker=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096"),
    value_serializer="json",
    store="rocksdb://",
)

# Топики
products_topic = app.topic("products", value_type=Product)
filtered_topic = app.topic("filtered-products", value_type=FilteredProduct)


def load_banned_products():
    """Загрузка списка запрещённых товаров из файла"""
    banned_file = "data/banned_products.json"
    if os.path.exists(banned_file):
        with open(banned_file, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()


@app.agent(products_topic)
async def process_products(stream):
    """Фильтрация запрещённых товаров"""
    banned_products = load_banned_products()
    async for product in stream:
        if product.product_id in banned_products:
            logger.warning(f"⛔ Запрещён: {product.name} (ID: {product.product_id})")
            continue

        filtered = FilteredProduct(
            product_id=product.product_id,
            name=product.name,
            price=product.price,
            category=product.category,
            brand=product.brand,
            store_id=product.store_id,
        )
        await filtered_topic.send(key=product.product_id, value=filtered)
        logger.info(f"✅ Одобрен: {product.name} (ID: {product.product_id})")


@app.agent(filtered_topic)
async def log_filtered(stream):
    """Логирование отфильтрованных товаров"""
    async for product in stream:
        logger.info(f"📦 Отфильтрован: {product['name']}")


if __name__ == "__main__":
    logger.info("🚀 Запуск Faust-процессора...")
    app.main()