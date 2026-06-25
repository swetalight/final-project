#!/usr/bin/env python3
"""
Spark-аналитика из HDFS

Читает данные из HDFS, вычисляет статистику по категориям
и топ-10 дорогих товаров.
"""

import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc

# Загружаем .env
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SparkAnalytics:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName(os.getenv("SPARK_APP_NAME", "MarketplaceAnalytics")) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.master", os.getenv("SPARK_MASTER", "local[*]")) \
            .config("spark.hadoop.fs.defaultFS", os.getenv("HDFS_URL", "hdfs://localhost:9000")) \
            .getOrCreate()
        logger.info("✅ Spark сессия создана")

    def analyze_from_hdfs(self):
        """Аналитика из HDFS"""
        hdfs_path = os.getenv("HDFS_PATH", "/data/products")
        full_path = f"{os.getenv('HDFS_URL', 'hdfs://localhost:9000')}{hdfs_path}"

        try:
            df = self.spark.read.json(full_path)
            logger.info(f"📊 Данные загружены из HDFS: {full_path}")

            # Статистика по категориям
            category_stats = df.groupBy("category").agg(
                count("*").alias("total_products"),
                avg("price.amount").alias("avg_price")
            ).orderBy(desc("total_products"))

            logger.info("📊 Статистика по категориям:")
            category_stats.show()

            # Топ-10 дорогих товаров
            top_expensive = df.orderBy(desc("price.amount")).limit(10)
            logger.info("💰 Топ-10 дорогих товаров:")
            top_expensive.select("name", "price.amount").show()

            # Сохранение результатов
            output_path = f"{os.getenv('HDFS_URL', 'hdfs://localhost:9000')}/analytics/category_stats"
            category_stats.write.mode("overwrite").json(output_path)
            logger.info(f"✅ Результаты сохранены в: {output_path}")

            return category_stats, top_expensive

        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return None, None


if __name__ == "__main__":
    analytics = SparkAnalytics()
    analytics.analyze_from_hdfs()