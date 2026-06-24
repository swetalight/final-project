#!/bin/bash
# =====================================================
# Инициализация Kafka: топики, пользователи, ACL
# =====================================================

BOOTSTRAP="kafka1:9092"
ADMIN_CONFIG="/tmp/admin.properties"

echo "========================================="
echo "Инициализация Kafka кластера"
echo "========================================="

echo "⏳ Ожидание запуска Kafka..."
sleep 30

# Создание admin.properties
cat > /tmp/admin.properties << EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="uosieQu6ACie0eichoo5ohdeeWaepowa";
ssl.truststore.location=/etc/kafka/secrets/kafka.truststore.pem
ssl.truststore.type=PEM
ssl.keystore.location=/etc/kafka/secrets/kafka.keystore.pem
ssl.keystore.type=PEM
