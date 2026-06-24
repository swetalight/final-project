#!/usr/bin/env python3
"""Faust-процессор для фильтрации запрещённых товаров"""

import json
import os
import faust
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    store_id: str


app = faust.App(
    "marketplace-processor",
    broker="kafka://kafka1:9093,kafka2:9093,kafka3:9093",
    value_serializer="json",
    store="rocksdb://",
)

products_topic = app.topic("products", value_type=Product)
filtered_topic = app.topic("filtered-products")


def load_banned_products():
    banned_file = "data/banned_products.json"
    if os.path.exists(banned_file):
        with open(banned_file, "r") as f:
            return set(json.load(f))
    return set()


banned_products = load_banned_products()


@app.agent(products_topic)
async def process_products(stream):
    async for product in stream:
        if product.product_id in banned_products:
            logger.warning(f"⛔ Запрещён: {product.name} (ID: {product.product_id})")
            continue
        await filtered_topic.send(key=product.product_id, value=product.to_dict())
        logger.info(f"✅ Одобрен: {product.name} (ID: {product.product_id})")


@app.agent(filtered_topic)
async def log_filtered(stream):
    async for product in stream:
        logger.info(f"📦 Отфильтрован: {product['name']}")


if __name__ == "__main__":
    logger.info("🚀 Запуск Faust-процессора...")
    app.main()