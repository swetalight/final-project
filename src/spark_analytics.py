#!/usr/bin/env python3
"""Spark-аналитика из HDFS"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SparkAnalytics:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("MarketplaceAnalytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.master", "local[*]") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .getOrCreate()
        logger.info("✅ Spark сессия создана")

    def analyze_from_hdfs(self, hdfs_path="hdfs://localhost:9000/data/products"):
        """Аналитика из HDFS"""
        try:
            df = self.spark.read.json(hdfs_path)
            logger.info(f"📊 Данные загружены из HDFS: {hdfs_path}")

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

            return category_stats, top_expensive
        except Exception as e:
            logger.error(f"❌ Ошибка: {e}")
            return None, None


if __name__ == "__main__":
    analytics = SparkAnalytics()
    analytics.analyze_from_hdfs()