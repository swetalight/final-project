Финальный проект: Аналитическая платформа для маркетплейса
Полный отчет по всем компонентам
📋 Содержание
Бизнес-контекст

Архитектура решения

Шаг 1. Источники данных

Шаг 2. Kafka кластер

Шаг 3. Аналитическая система

Шаг 4. Потоковая обработка

Шаг 5. Хранилище данных

Шаг 6. Мониторинг

Шаг 7. Документация

Критерии выполненной работы

📖 Бизнес-контекст
«Покупай выгодно» — платформа электронной коммерции. Ближайшая цель маркетплейса — улучшить клиентский опыт и оптимизировать бизнес-процессы. Для этого команда внедряет аналитическую платформу, которая собирает данные о взаимодействии клиентов с сайтом: просмотры товаров, добавление в корзину, покупки и отзывы.

Результат: персонализированные рекомендации, релевантная реклама, улучшение ассортимента и повышение конверсии.

🏗️ Архитектура решения























📂 Структура проекта
final-project/
├── README.md                          # 📄 Документация проекта
├── docker-compose.yml                 # 🐳 Основной кластер Kafka (6 брокеров + MirrorMaker 2)
├── docker-compose-monitoring.yml      # 📊 Мониторинг (Prometheus + Grafana + Alertmanager)
├── mirror-maker.properties            # 🔁 Конфигурация MirrorMaker 2 для репликации
├── config.json                        # ⚙️ Конфигурация сервисов (с переменными ${VAR})
├── requirements.txt                   # 🐍 Python зависимости
├── .env.example                       # 🔐 Шаблон переменных окружения (в Git)
├── .env                               # 🔐 Реальные переменные (НЕ в Git!)
├── .gitignore                         # 🚫 Игнорируемые файлы
│
├── data/
│   ├── products.json                  # 📦 Товары (10 шт) для SHOP API
│   └── banned_products.json           # 🚫 Список запрещённых товаров
│
├── src/
│   ├── __init__.py                    # 📦 Инициализация модуля
│   ├── shop_api.py                    # 🏪 SHOP API — отправка товаров в Kafka
│   ├── client_api.py                  # 👤 CLIENT API — поиск и рекомендации
│   ├── faust_processor.py             # 🔄 Потоковая обработка (фильтрация Faust)
│   ├── spark_analytics.py             # 📊 Аналитика (Spark + HDFS)
│   ├── elasticsearch_consumer.py      # 🔍 Запись данных в Elasticsearch
│   └── banned_cli.py                  # 🚫 CLI управление запрещёнными товарами
│
├── scripts/
│   ├── generate-certs.sh              # 🔐 Генерация SSL сертификатов
│   ├── init-kafka.sh                  # 🏗️ Создание топиков и ACL
│   └── init-acl.sh                    # 🔐 Настройка ACL
│
├── secrets/                           # 🔐 SSL сертификаты (НЕ в Git!)
│   ├── kafka.truststore.pem           # CA сертификат
│   ├── kafka.keystore.pem             # Сертификат клиента
│   └── kafka.keystore.key             # Ключ клиента
│
├── kafka_cck1/                        # 🔐 SSL для брокера 1
│   ├── kafka_jaas.conf
│   ├── data/
│   └── ssl/
│       ├── CAs/
│       ├── kafka.keystore.pem
│       ├── kafka.keystore.key
│       └── kafka.truststore.pem
│
├── kafka_cck2/                        # 🔐 SSL для брокера 2
├── kafka_cck3/                        # 🔐 SSL для брокера 3
├── kafka_cck4/                        # 🔐 SSL для брокера 4 (второй кластер)
├── kafka_cck5/                        # 🔐 SSL для брокера 5 (второй кластер)
├── kafka_cck6/                        # 🔐 SSL для брокера 6 (второй кластер)
│
├── prometheus/
│   ├── prometheus.yml                 # 📊 Конфиг Prometheus
│   └── alerts.yml                     # 🔔 Алерты Alertmanager
│
├── grafana-dashboards/
│   └── kafka-dashboard.json           # 📊 Дашборд Grafana
│
├── hadoop_data/                       # 💾 Данные HDFS (создаётся автоматически)
│   ├── namenode/
│   └── datanode/
│
├── kafkaui.yml                        # 🖥️ Конфиг Kafbat UI
└── kafka_jaas.conf                    # 🔐 JAAS конфиг аутентификации

Шаг 1. Источники данных
1.1. SHOP API
Назначение: эмуляция отправки товаров от магазинов в Kafka.

Реализация:

Чтение данных из файла data/products.json

Отправка товаров в Kafka-топик products

Формат данных — JSON согласно спецификации

Структура товара:

json
{
  "product_id": "12345",
  "name": "Умные часы XYZ",
  "description": "Умные часы с функцией мониторинга здоровья, GPS и уведомлениями.",
  "price": {
    "amount": 4999.99,
    "currency": "RUB"
  },
  "category": "Электроника",
  "brand": "XYZ",
  "stock": {
    "available": 150,
    "reserved": 20
  },
  "sku": "XYZ-12345",
  "tags": ["умные часы", "гаджеты", "технологии"],
  "images": [
    {
      "url": "https://example.com/images/product1.jpg",
      "alt": "Умные часы XYZ - вид спереди"
    },
    {
      "url": "https://example.com/images/product1_side.jpg",
      "alt": "Умные часы XYZ - вид сбоку"
    }
  ],
  "specifications": {
    "weight": "50g",
    "dimensions": "42mm x 36mm x 10mm",
    "battery_life": "24 hours",
    "water_resistance": "IP68"
  },
  "created_at": "2023-10-01T12:00:00Z",
  "updated_at": "2023-10-10T15:30:00Z",
  "index": "products",
  "store_id": "store_001"
}
Код: src/shop_api.py

python
class ShopAPI:
    def __init__(self):
        # Подключение к Kafka с SSL
        kafka_config = {
            "bootstrap.servers": "localhost:9092,localhost:9094,localhost:9096",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": "shop",
            "sasl.password": "shop123",
            "ssl.ca.location": "/path/to/ca.pem",
        }
        self.producer = Producer(kafka_config)
    
    def send_products(self):
        products = json.load(open("data/products.json"))
        for product in products:
            self.producer.produce(
                topic="products",
                key=product["product_id"],
                value=json.dumps(product)
            )
Результат выполнения:

text
✅ Подключение к Kafka: localhost:9092,localhost:9094,localhost:9096
📦 Отправка 10 товаров...
[1/10] Умные часы XYZ
✅ Доставлено: products [p0] o0
[2/10] Смартфон ABC Pro
✅ Доставлено: products [p1] o0
...
[10/10] Запрещённый товар
✅ Все товары отправлены
1.2. CLIENT API
Назначение: эмуляция запросов клиентов к платформе.

Команды:

search <имя> — поиск информации о товаре по его имени

rec <user_id> — получение персонализированных рекомендаций

Реализация:

Команды вводятся в терминале

Запросы отправляются в Kafka-топик client-requests

Данные ищутся в Elasticsearch

Код: src/client_api.py

python
class ClientAPI:
    def search_product(self, query):
        # Отправка запроса в Kafka
        request = {"request_id": str(uuid.uuid4()), "type": "search", "query": query}
        self.producer.produce(topic="client-requests", value=json.dumps(request))
        
        # Поиск в Elasticsearch
        result = self.es.search(index="products", body={"query": {"match": {"name": query}}})
        return result
    
    def get_recommendations(self, user_id):
        # Отправка запроса в Kafka
        request = {"request_id": str(uuid.uuid4()), "type": "recommendation", "user_id": user_id}
        self.producer.produce(topic="client-requests", value=json.dumps(request))
        
        # Имитация рекомендаций
        return recommendations
Результат выполнения:

text
📌 Команда (search <имя> / rec <user_id> / exit): search Умные часы
🔍 Поиск: 'Умные часы'
  📦 Умные часы XYZ — 4999.99 RUB

📌 Команда (search <имя> / rec <user_id> / exit): rec user_123
🎯 Рекомендации для пользователя: user_123
  ⭐ Умные часы XYZ (score: 0.95)
  ⭐ Смартфон ABC Pro (score: 0.87)
  ⭐ Наушники XYZ Air (score: 0.82)
Шаг 2. Kafka кластер
2.1. Развертывание кластера
Инфраструктура:

Компонент	Количество	Порт
Основной кластер	3 брокера	9092, 9094, 9096
Второй кластер	3 брокера	9192, 9194, 9196
MirrorMaker 2	1 сервис	-
Schema Registry	1	8081
Docker Compose: docker-compose.yml

2.2. Безопасность (TLS)
Генерация сертификатов: scripts/generate-certs.sh  

bash
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
SSL настройки:

security.protocol: SASL_SSL

sasl.mechanism: SCRAM-SHA-512

Сертификаты в формате PEM

2.3. Топики
Топик	Партиции	Реплики	min.insync.replicas	Назначение
products	3	2	2	Товары от магазинов
client-requests	3	2	2	Запросы от клиентов
filtered-products	3	2	2	Отфильтрованные товары
recommendations	3	2	2	Рекомендации
banned-products	1	2	2	Запрещённые товары
Создание топиков: scripts/init-kafka.sh

bash
kafka-topics.sh --create --topic products \
    --partitions 3 --replication-factor 2 \
    --config min.insync.replicas=2 \
    --bootstrap-server kafka1:9092
2.4. ACL (управление доступом)
Пользователь	Топик	Операции	Назначение
shop	products	Write, Describe	Отправка товаров
client	client-requests	Write, Describe	Отправка запросов
processor	products	Read, Describe	Чтение для фильтрации
processor	filtered-products	Write, Describe	Запись отфильтрованных
analytics	filtered-products	Read, Describe	Чтение для аналитики
analytics	recommendations	Write, Describe	Запись рекомендаций
Настройка ACL: scripts/init-acl.sh

bash
kafka-acls.sh --add \
    --allow-principal User:shop \
    --operation Write --operation Describe \
    --topic products \
    --bootstrap-server kafka1:9092
2.5. Отказоустойчивость
Репликация:

replication-factor: 2 — данные хранятся на 2 брокерах

min.insync.replicas: 2 — подтверждение от 2 реплик

MirrorMaker 2: дублирование данных на второй кластер

properties
clusters=source, target
topics=products,client-requests,filtered-products,recommendations
sync.topic.acls.enabled=true
sync.topic.configs.enabled=true
Результат репликации:

text
✅ products реплицирован на второй кластер
✅ filtered-products реплицирован на второй кластер
✅ recommendations реплицирован на второй кластер
🔁 MirrorMaker 2 — Репликация данных
2.6. Конфигурация MirrorMaker 2
mirror-maker.properties:

properties
clusters=source, target

source.bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092
source.security.protocol=SASL_SSL
source.sasl.mechanism=PLAIN
source.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="uosieQu6ACie0eichoo5ohdeeWaepowa";
source.ssl.truststore.location=/etc/kafka/secrets/truststore.pem
source.ssl.truststore.type=PEM
source.ssl.keystore.location=/etc/kafka/secrets/keystore.pem
source.ssl.keystore.type=PEM

target.bootstrap.servers=kafka4:9092,kafka5:9092,kafka6:9092
target.security.protocol=SASL_SSL
target.sasl.mechanism=PLAIN
target.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="uosieQu6ACie0eichoo5ohdeeWaepowa";
target.ssl.truststore.location=/etc/kafka/secrets/truststore-mirror.pem
target.ssl.truststore.type=PEM
target.ssl.keystore.location=/etc/kafka/secrets/keystore-mirror.pem
target.ssl.keystore.type=PEM

topics=products,client-requests,filtered-products,recommendations
topics.exclude=__.*,_.*,*.replica

sync.topic.acls.enabled=true
sync.topic.configs.enabled=true
consumer.group.prefix=mirror-

source.consumer.max.poll.records=500
source.consumer.fetch.max.bytes=52428800
source.consumer.fetch.max.wait.ms=500

target.producer.acks=all
target.producer.enable.idempotence=true
target.producer.compression.type=snappy

checkpoints.topic=mirror-checkpoints
heartbeats.topic=mirror-heartbeats
offset-syncs.topic=mirror-offset-syncs

heartbeat.interval.seconds=5
checkpoint.interval.seconds=60
offset.syncs.interval.seconds=10
2.7. Проверка MirrorMaker 2
Проверка логов:

bash
docker-compose logs -f mirror-maker
text
⏳ Ожидание запуска Kafka...
[2026-06-24 12:00:00] INFO MirrorMaker: Starting...
[2026-06-24 12:00:05] INFO Replicating topic: products
[2026-06-24 12:00:05] INFO Replicating topic: client-requests
[2026-06-24 12:00:05] INFO Replicating topic: filtered-products
[2026-06-24 12:00:05] INFO Replicating topic: recommendations
Проверка топиков во втором кластере:

bash
docker exec kafka4 /opt/kafka/bin/kafka-topics.sh --list \
    --bootstrap-server kafka4:9092 \
    --command-config /tmp/admin.properties
text
products           ✅
client-requests    ✅
filtered-products  ✅
recommendations    ✅
Проверка данных во втором кластере:

bash
docker exec kafka4 /opt/kafka/bin/kafka-console-consumer.sh \
    --topic products \
    --from-beginning \
    --bootstrap-server kafka4:9092 \
    --max-messages 5
text
{"product_id":"12345","name":"Умные часы XYZ",...}
{"product_id":"12346","name":"Смартфон ABC Pro",...}
Шаг 3. Аналитическая система
3.1. HDFS (Data Lake)
Развертывание: NameNode + DataNode в Docker

yaml
namenode:
  image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
  ports:
    - "9870:9870"  # Web UI
    - "9000:9000"  # HDFS порт

datanode:
  image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
  ports:
    - "9864:9864"
Загрузка данных:

bash
# Создать директорию
docker exec -it namenode hdfs dfs -mkdir -p /data/products

# Загрузить данные
docker exec -it namenode hdfs dfs -put /tmp/products.json /data/products/

# Проверить
docker exec -it namenode hdfs dfs -ls /data/products/
# Found 1 items: products.json
3.2. Spark аналитика
Код: src/spark_analytics.py

python
class SparkAnalytics:
    def analyze_from_hdfs(self):
        # Чтение из HDFS
        df = self.spark.read.json("hdfs://localhost:9000/data/products")
        
        # 1. Статистика по категориям
        category_stats = df.groupBy("category").agg(
            count("*").alias("total_products"),
            avg("price.amount").alias("avg_price"),
            sum("stock.available").alias("total_stock")
        ).orderBy(desc("total_products"))
        
        # 2. Топ-10 дорогих товаров
        top_expensive = df.orderBy(desc("price.amount")).limit(10)
        
        # 3. Сохранение рекомендаций в топик
        recommendations = df.filter(col("stock.available") > 10) \
            .orderBy(desc("price.amount")) \
            .limit(5)
        
        # Запись в Kafka
        recommendations.write.format("kafka") \
            .option("topic", "recommendations") \
            .save()
Результат аналитики:

text
📊 Статистика по категориям:
+----------+--------------+---------+------------+
|category  |total_products|avg_price|total_stock |
+----------+--------------+---------+------------+
|Электроника|4             |...      |...         |
|Дом и сад  |3             |...      |...         |
|Транспорт  |1             |...      |...         |
|Одежда и обувь|1          |...      |...         |
|Книги      |1             |...      |...         |
+----------+--------------+---------+------------+

💰 Топ-10 дорогих товаров:
+------------------+--------+
|name              |amount  |
+------------------+--------+
|Ноутбук ABC Gaming X|149999.99|
|Смартфон ABC Pro  |89999.99|
|Кофеварка XYZ Barista|45999.99|
|Электросамокат XYZ Pro|34999.99|
|Робот-пылесос XYZ RoboClean|24999.99|
...
Шаг 4. Потоковая обработка
4.1. Faust Processor
Назначение: фильтрация запрещённых товаров в реальном времени.

Код: src/faust_processor.py

python
class Product(faust.Record):
    product_id: str
    name: str
    price: dict
    category: str
    brand: str
    stock: dict
    store_id: str

app = faust.App("marketplace-processor", broker="kafka://kafka1:9093")

products_topic = app.topic("products", value_type=Product)
filtered_topic = app.topic("filtered-products")
banned_products = load_banned_products()

@app.agent(products_topic)
async def process_products(stream):
    async for product in stream:
        if product.product_id in banned_products:
            logger.warning(f"⛔ Запрещён: {product.name}")
            continue
        await filtered_topic.send(key=product.product_id, value=product.to_dict())
        logger.info(f"✅ Одобрен: {product.name}")
Результат выполнения:

text
✅ Одобрен: Умные часы XYZ (ID: 12345)
✅ Одобрен: Смартфон ABC Pro (ID: 12346)
⛔ Запрещён: Запрещённый товар (ID: 99999)
✅ Одобрен: Наушники XYZ Air (ID: 12347)
4.2. Управление запрещёнными товарами
CLI: src/banned_cli.py

bash
python src/banned_cli.py

> add 99999
✅ Товар 99999 добавлен в список

> add 88888
✅ Товар 88888 добавлен в список

> list
📋 Запрещённые товары:
  - 88888
  - 99999

> remove 88888
✅ Товар 88888 удалён из списка

> list
📋 Запрещённые товары:
  - 99999
Шаг 5. Хранилище данных
5.1. Elasticsearch
Назначение: хранение и поиск данных о товарах.

Развертывание: Elasticsearch 8.11.0 в Docker

yaml
elasticsearch:
  image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
  environment:
    - discovery.type=single-node
    - xpack.security.enabled=false
  ports:
    - "9200:9200"
Consumer: src/elasticsearch_consumer.py

python
class ElasticsearchConsumer:
    def process_messages(self):
        for message in self.consumer:
            product = message.value
            self.es.index(
                index="products",
                id=product["product_id"],
                body=product
            )
            logger.info(f"✅ Индексирован: {product['name']}")
Результат:

text
✅ Индексирован: Умные часы XYZ
✅ Индексирован: Смартфон ABC Pro
✅ Индексирован: Наушники XYZ Air
✅ Индексирован: Ноутбук ABC Gaming X
Проверка данных:

bash
curl -X GET "localhost:9200/products/_search?pretty" | head -30
Шаг 6. Мониторинг
6.1. Prometheus
Конфигурация: prometheus/prometheus.yml

yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets:
        - kafka1:5556
        - kafka2:5556
        - kafka3:5556
  
  - job_name: 'elasticsearch'
    static_configs:
      - targets:
        - elasticsearch:9200
  
  - job_name: 'schema-registry'
    static_configs:
      - targets:
        - schema-registry:8081
6.2. Grafana
Дашборд: grafana-dashboards/kafka-dashboard.json

URL: http://localhost:3000
Логин: admin
Пароль: admin

Метрики на дашборде:

Количество сообщений в топиках

Задержка потребителей

Использование диска брокерами

Количество активных соединений

6.3. Alertmanager
Алерты: prometheus/alerts.yml

yaml
groups:
  - name: kafka_alerts
    rules:
      - alert: KafkaBrokerDown
        expr: up{job="kafka"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka брокер {{ $labels.instance }} недоступен"
          description: "Брокер Kafka не отвечает более 1 минуты"
      
      - alert: KafkaHighLag
        expr: kafka_consumer_lag > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Высокая задержка в Kafka"
          description: "Задержка потребителя превышает 1000 сообщений"
      
      - alert: KafkaTopicUnderReplicated
        expr: kafka_topic_partition_under_replicated_partitions > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Недостаточная репликация топика"
Шаг 7. Документация
7.1. README.md
Полная документация включает:

Инструкция по запуску:

🚀 Команды для создания структуры
bash
# Создать все папки
mkdir -p data src scripts secrets prometheus grafana-dashboards hadoop_data

# Создать файлы
touch README.md docker-compose.yml docker-compose-monitoring.yml \
      mirror-maker.properties config.json requirements.txt .env.example .gitignore

# Создать файлы в src
touch src/__init__.py src/shop_api.py src/client_api.py \
      src/faust_processor.py src/spark_analytics.py \
      src/elasticsearch_consumer.py src/banned_cli.py

# Создать файлы в scripts
touch scripts/generate-certs.sh scripts/init-kafka.sh scripts/init-acl.sh

# Сделать скрипты исполняемыми
chmod +x scripts/*.sh

bash
# 1. Генерация сертификатов
./scripts/generate-certs.sh

# 2. Скопировать шаблон
cp .env.example .env

 Отредактировать .env при необходимости

 Установить зависимости
pip install -r requirements.txt


# 3. Запуск кластера
docker-compose up -d

# 4. Инициализация топиков и ACL
docker-compose exec kafka-init bash /scripts/init-kafka.sh

# 5. Установка зависимостей
pip install -r requirements.txt

# 6. Запуск SHOP API
python src/shop_api.py

# 7. Запуск Faust-процессора
faust -A src.faust_processor worker -l info

# 8. Запуск CLIENT API
python src/client_api.py

# 9. Запуск мониторинга
docker-compose -f docker-compose-monitoring.yml up -d
Используемые технологии:

Apache Kafka 4.1.2

Faust 0.11.3

Apache Spark 3.5.0

HDFS 3.2.1

Elasticsearch 8.11.0

Prometheus + Grafana

Описание реализации:

Архитектура микросервисов

Потоковая обработка данных

Аналитика в реальном времени

Мониторинг и оповещения

Пояснение к реализации

1. Общая архитектура
Проект реализует аналитическую платформу для маркетплейса на основе микросервисной архитектуры с использованием Apache Kafka как центральной шины данных.

Ключевые принципы:
Асинхронная передача данных через Kafka

Масштабируемость — каждый компонент независим

Отказоустойчивость — репликация и MirrorMaker 2

Безопасность — TLS + ACL

Мониторинг — Prometheus + Grafana

2. Источники данных (Шаг 1)
SHOP API (src/shop_api.py)
Что делает: эмулирует работу магазинов, отправляющих товары в систему.

Как работает:

Читает JSON-файл data/products.json с 10 товарами

Для каждого товара создает сообщение в формате JSON

Отправляет сообщение в Kafka-топик products

Использует SSL/TLS для безопасной передачи

Почему так:

Файл — простой способ эмуляции без создания веб-сервиса

JSON — стандартный формат для обмена данными

Kafka — гарантирует доставку и упорядочивание

CLIENT API (src/client_api.py)
Что делает: эмулирует запросы клиентов (поиск и рекомендации).

Как работает:

Принимает команды из терминала (search и rec)

Отправляет запросы в Kafka-топик client-requests

Выполняет поиск в Elasticsearch

Возвращает результаты пользователю

Почему так:

Терминальный интерфейс — простейший способ демонстрации

Kafka — для аналитики запросов

Elasticsearch — для быстрого поиска

3. Kafka кластер (Шаг 2)
Развертывание
Что сделано: развернуто 6 брокеров в Docker (3 в основном кластере, 3 во втором).

Как работает:

Каждый брокер — отдельный контейнер

Брокеры общаются между собой по протоколу SASL_SSL

Используется режим KRaft (без ZooKeeper)

Почему так:

Docker — легковесная контейнеризация

SASL_SSL — шифрование и аутентификация

KRaft — современный режим Kafka без ZooKeeper

Безопасность (TLS)
Что сделано: сгенерированы SSL-сертификаты для каждого брокера.

Как работает:

Создан корневой CA-сертификат

Для каждого брокера выпущен сертификат

Настроено взаимное SSL-шифрование

Почему так:

TLS — стандарт безопасности в распределенных системах

Взаимная аутентификация — защита от подделки брокеров

Топики и ACL
Что сделано: созданы 5 топиков и настроены ACL для 4 пользователей.

Как работает:

Топики созданы с replication-factor=2 и min.insync.replicas=2

ACL ограничивают доступ: кто может писать/читать в какие топики

Почему так:

Репликация — отказоустойчивость

ACL — безопасность и разделение прав

MirrorMaker 2
Что делает: реплицирует данные из основного кластера во второй.

Как работает:

Читает сообщения из топиков в основном кластере

Записывает их в те же топики во втором кластере

Работает асинхронно с заданными интервалами

Почему так:

Дублирование — отказоустойчивость

Асинхронность — не влияет на производительность основного кластера

4. Аналитическая система (Шаг 3)
HDFS (Data Lake)
Что сделано: развернут HDFS в Docker (NameNode + DataNode).

Как работает:

Данные из второго кластера Kafka загружаются в HDFS

Хранятся в распределенном файловом хранилище

Почему так:

HDFS — стандартное хранилище для больших данных

Data Lake — позволяет хранить неструктурированные данные

Spark аналитика
Что делает: анализирует данные из HDFS.

Как работает:

Читает данные из HDFS

Вычисляет:

Количество товаров по категориям

Среднюю цену по категориям

Топ-10 дорогих товаров

Записывает рекомендации в топик Kafka

Почему так:

Spark — мощный движок для обработки больших данных

Рекомендации — ключевой бизнес-результат

5. Потоковая обработка (Шаг 4)
Faust Processor
Что делает: фильтрует запрещённые товары в реальном времени.

Как работает:

Подписывается на топик products

Для каждого товара проверяет, есть ли он в списке запрещённых

Если товар запрещён — логирует и пропускает

Если разрешён — отправляет в топик filtered-products

Почему так:

Faust — Python-библиотека для потоковой обработки

Реальное время — мгновенная фильтрация

Список запрещённых — управляется через CLI

Управление запрещёнными товарами (banned_cli.py)
Что делает: позволяет добавлять/удалять товары из списка запрещённых.

Как работает:

Команды add <id> и remove <id> через терминал

Данные сохраняются в data/banned_products.json

Faust перечитывает список при запуске

Почему так:

CLI — простой интерфейс для управления

JSON-файл — легко редактировать и хранить в Git

6. Хранилище данных (Шаг 5)
Elasticsearch
Что делает: хранит и индексирует данные для поиска.

Как работает:

Читает отфильтрованные товары из топика filtered-products

Индексирует их в Elasticsearch

CLIENT API выполняет поиск по этим данным

Почему так:

Elasticsearch — быстрый полнотекстовый поиск

Индексация — моментальный доступ к данным

7. Мониторинг (Шаг 6)
Prometheus
Что делает: собирает метрики со всех компонентов.

Как работает:

JMX Exporter собирает метрики Kafka

Prometheus опрашивает exporters по расписанию

Почему так:

Prometheus — стандарт сбора метрик

JMX Exporter — интеграция с Kafka

Grafana
Что делает: визуализирует метрики на дашбордах.

Как работает:

Подключается к Prometheus как источнику данных

Отображает графики и алерты

Почему так:

Grafana — удобная визуализация

Интерактивные дашборды

Alertmanager
Что делает: отправляет оповещения при сбоях.

Как работает:

Получает алерты от Prometheus

Отправляет уведомления (Telegram, email)

Почему так:

Alertmanager — стандартный компонент Prometheus

Оповещения — быстрая реакция на проблемы

8. Документация (Шаг 7)
README.md
Что содержит:

Инструкцию по запуску

Список технологий

Описание архитектуры

Почему так:

Документация — обязательный элемент любого проекта

Инструкция — упрощает запуск

📊 Потоки данных
Поток 1: SHOP API → Kafka → Faust → Elasticsearch
text
SHOP API → products (topic) → Faust → filtered-products (topic) → Elasticsearch
Поток 2: CLIENT API → Kafka → ES
text
CLIENT API → client-requests (topic) → Elasticsearch
Поток 3: Kafka → MirrorMaker → HDFS → Spark → recommendations
text
products (topic) → MirrorMaker → second cluster → HDFS → Spark → recommendations (topic)
Поток 4: Kafka → Prometheus → Grafana
text
Kafka → JMX Exporter → Prometheus → Grafana
✅ Ключевые технические решения
Решение	Почему
Apache Kafka	Центральная шина данных, надежная доставка
SASL_SSL	Безопасность передачи данных
ACL	Разграничение доступа к топикам
MirrorMaker 2	Отказоустойчивость, дублирование данных
Faust	Потоковая обработка на Python
Spark	Обработка больших данных
HDFS	Data Lake, хранение больших объемов
Elasticsearch	Быстрый поиск данных
Prometheus + Grafana	Мониторинг и визуализация