# Создание корневого CA
openssl req -new -x509 -days 365 -nodes -newkey rsa:2048 \
    -keyout ca.key -out ca.crt -subj "/CN=KafkaSSL-CA"

# Создание сертификатов для каждого брокера
for i in 1 2 3 4 5 6; do
    openssl req -new -newkey rsa:2048 -nodes \
        -keyout kafka_cck$i/ssl/kafka$i.key \
        -out kafka_cck$i/ssl/kafka$i.csr \
        -subj "/CN=kafka$i"
    openssl x509 -req -days 365 \
        -in kafka_cck$i/ssl/kafka$i.csr \
        -CA kafka_cck$i/ssl/CAs/ca.crt \
        -CAkey kafka_cck$i/ssl/CAs/ca.key \
        -CAcreateserial \
        -out kafka_cck$i/ssl/kafka$i.crt
done