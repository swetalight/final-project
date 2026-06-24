cat > scripts/init-acl.sh << 'EOF'
#!/bin/bash
# =====================================================
# Проверка ACL
# =====================================================

BOOTSTRAP="kafka1:9092"
ADMIN_CONFIG="/tmp/admin.properties"

echo "========================================="
echo "🔐 Проверка ACL"
echo "========================================="

echo ""
echo "📋 Текущие ACL:"
docker exec kafka1 /opt/kafka/bin/kafka-acls.sh --list \
    --bootstrap-server $BOOTSTRAP \
    --command-config /tmp/admin.properties 2>/dev/null | grep -E "User:(shop|client|processor|analytics)" || echo "   ⚠️ ACL не найдены"

echo ""
echo "👤 Пользователи SCRAM:"
docker exec kafka1 /opt/kafka/bin/kafka-configs.sh --describe \
    --bootstrap-server $BOOTSTRAP \
    --entity-type users \
    --command-config /tmp/admin.properties 2>/dev/null | grep -E "shop|client|processor|analytics"

echo ""
echo "📦 Топики:"
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --list \
    --bootstrap-server $BOOTSTRAP \
    --command-config /tmp/admin.properties 2>/dev/null

echo ""
echo "========================================="
echo "✅ Проверка завершена"
echo "========================================="
EOF

chmod +x scripts/init-acl.sh