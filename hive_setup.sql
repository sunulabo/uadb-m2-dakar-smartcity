-- hive_setup.sql — Tables Hive Dakar Smart City
-- Exécuter : docker exec hive-metastore hive -f hive_setup.sql

CREATE DATABASE IF NOT EXISTS dakar_smart_city
  COMMENT 'Mobilité Urbaine Dakar — UADB 2025-2026';

USE dakar_smart_city;

-- ── Table principale : Gold Layer mobilité ───────────────────────────────
CREATE TABLE IF NOT EXISTS mobilite_gold (
  vehicle_secure STRING  COMMENT 'SHA-256 — plaque anonymisée',
  vehicle_type   STRING,
  ligne_id       STRING,
  latitude       DOUBLE,
  longitude      DOUBLE,
  speed_kmh      DOUBLE,
  passagers      INT,
  event_ts       TIMESTAMP
)
PARTITIONED BY (date_obs STRING, ligne_part STRING)
STORED AS ORC
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ── Table analytics : congestion par fenêtre ─────────────────────────────
CREATE TABLE IF NOT EXISTS congestion_historique (
  ligne_id          STRING,
  vitesse_moy       DOUBLE,
  vitesse_predite   DOUBLE,
  ecart_vitesse     DOUBLE,
  nb_vehicules      BIGINT,
  passagers_moy     DOUBLE,
  niveau_congestion STRING,
  periode_debut     TIMESTAMP
)
STORED AS ORC;

-- ── Table monitoring drift ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_drift_history (
  periode       TIMESTAMP,
  rmse_horaire  DOUBLE,
  r2_horaire    DOUBLE,
  seuil_alerte  DOUBLE DEFAULT 15.0
)
STORED AS ORC;

-- ── Vue : Congestion en cours par ligne ──────────────────────────────────
CREATE OR REPLACE VIEW vue_etat_trafic AS
SELECT
  ligne_id,
  AVG(COALESCE(speed_kmh, 0))  AS vitesse_moy_km_h,
  COUNT(vehicle_secure)         AS nb_vehicules_actifs,
  AVG(COALESCE(passagers, 0))  AS passagers_moy,
  CASE
    WHEN AVG(speed_kmh) < 5  THEN 'CRITIQUE'
    WHEN AVG(speed_kmh) < 15 THEN 'MODERE'
    ELSE 'FLUIDE'
  END AS etat_trafic
FROM mobilite_gold
WHERE event_ts >= DATE_SUB(CURRENT_TIMESTAMP, 1/96.0)  -- 15 dernières minutes
GROUP BY ligne_id
ORDER BY vitesse_moy_km_h ASC;

-- ── Vue : Drift modèle (RMSE > 15 = réentraînement nécessaire) ──────────
CREATE OR REPLACE VIEW vue_model_drift AS
SELECT
  window(event_ts, '1 hour')                                     AS periode,
  AVG(ABS(COALESCE(speed_kmh, 0) - COALESCE(speed_predicted, 25))) AS current_rmse
FROM mobilite_gold
WHERE event_ts >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY window(event_ts, '1 hour')
HAVING current_rmse > 15
ORDER BY periode DESC;

-- ── Vue : Performance par ligne et type de véhicule ─────────────────────
CREATE OR REPLACE VIEW vue_perf_lignes AS
SELECT
  ligne_id,
  vehicle_type,
  AVG(COALESCE(speed_kmh, 0))  AS vitesse_moy,
  AVG(COALESCE(passagers, 0))  AS taux_remplissage_moy,
  COUNT(vehicle_secure)         AS nb_enregistrements
FROM mobilite_gold
WHERE date_obs >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY ligne_id, vehicle_type
ORDER BY vitesse_moy ASC;
