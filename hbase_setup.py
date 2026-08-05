# hbase_setup.py — Tables HBase Dakar Smart City
import happybase
import logging

logger = logging.getLogger('HBaseSetup')


def create_dakar_tables():
    """Crée les 3 tables HBase nécessaires au projet Dakar Smart City."""
    conn = happybase.Connection('hbase', port=9090, timeout=10000)
    conn.open()

    tables = {
        # Positions GPS temps réel (TTL 1h — données opérationnelles)
        b'dakar:vehicules_temps_reel': {
            b'gps':     {'max_versions': 1, 'time_to_live': 3600},
            b'predict': {'max_versions': 5},   # vitesse_predite, conf_score
            b'meta':    {'max_versions': 1},   # vehicle_secure, type, ligne
        },
        # Alertes congestion actives (TTL 30min)
        b'dakar:alertes_trafic': {
            b'alerte': {'max_versions': 10, 'time_to_live': 1800},
        },
        # Historique drift modèle (30 jours de RMSE horaire)
        b'dakar:model_drift': {
            b'metrics': {'max_versions': 720},  # 30j × 24h
        },
    }

    existantes = [t.decode() for t in conn.tables()]

    for nom_b, fam in tables.items():
        nom = nom_b.decode()
        if nom not in existantes:
            conn.create_table(nom_b, fam)
            logger.info(f'Table {nom} créée ✓')
        else:
            logger.info(f'Table {nom} déjà existante')

    conn.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    create_dakar_tables()
