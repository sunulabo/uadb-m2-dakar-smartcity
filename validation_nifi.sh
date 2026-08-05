#!/bin/bash
# validation_nifi.sh — Validation NiFi post-configuration

echo "=== Validation NiFi — Dakar Smart City ==="

# 1. Vérifier que les topics Kafka existent
echo ""
echo "[1] Topics Kafka disponibles :"
docker exec kafka kafka-topics.sh --list --bootstrap-server kafka:9092
echo "    → Attendu : dakar_mobility_clean"

# 2. Consommer 5 messages de test
echo ""
echo "[2] Lecture de 5 messages du topic dakar_mobility_clean :"
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic dakar_mobility_clean \
  --max-messages 5 \
  --from-beginning

# 3. Vérifier que NiFi est accessible
echo ""
echo "[3] Test accès NiFi (http://localhost:8081) :"
curl -s -o /dev/null -w "HTTP Status : %{http_code}\n" http://localhost:8081/nifi/

# 4. Rappel livrable
echo ""
echo "[4] Livrable NiFi — Exporter le template :"
echo "    Menu NiFi → Process Group 'Dakar_Mobility_Ingestion'"
echo "    Clic droit → Download flow definition"
echo "    → Sauvegarder : template_nifi_dakar_equipeX.xml"
