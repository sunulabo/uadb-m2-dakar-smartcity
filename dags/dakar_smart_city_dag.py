# dags/dakar_smart_city_dag.py — DAG Airflow Dakar Smart City
# Monitoring drift RMSE horaire + réentraînement automatique RandomForest

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import subprocess
import logging

logger = logging.getLogger('dakar_dag')

default_args = {
    'owner':            'seye_ahmed',
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': True,
    'email':            ['dakar-smart-city@sonatel.sn'],
}


def check_model_drift(**ctx):
    """T1 — Vérifie le RMSE horaire via la vue Hive vue_model_drift.
    Si RMSE > 15 km/h sur plus de 3 périodes : déclencher réentraînement.
    """
    from pyhive import hive
    conn   = hive.Connection(host='hive-metastore', port=10000)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) AS nb_periodes_drift
        FROM dakar_smart_city.vue_model_drift
        WHERE current_rmse > 15
    """)
    nb_drift = cursor.fetchone()[0] or 0
    logger.info(f'Périodes en drift RMSE>15 : {nb_drift}')
    ctx['ti'].xcom_push(key='nb_drift', value=nb_drift)
    return 'trigger_retrain' if nb_drift > 3 else 'update_hbase'


def trigger_retrain(**ctx):
    """T2a — Réentraîne le modèle et met à jour le lien 'latest'.
    Lance spark-submit sur train_and_catalog.py.
    """
    result = subprocess.run(
        ['spark-submit', '--master', 'spark://spark-master:7077',
         '/opt/uadb/scripts/train_and_catalog.py'],
        capture_output=True, text=True, timeout=3600
    )
    if result.returncode != 0:
        raise Exception(f'Spark train failed: {result.stderr[-500:]}')
    logger.info('Modèle réentraîné et versionné ✓')


def update_hbase(**ctx):
    """T2b — Met à jour HBase avec les alertes de congestion actuelles.
    Lit vue_etat_trafic et pousse dans dakar:alertes_trafic.
    """
    from pyhive import hive
    import happybase
    from datetime import datetime

    conn_hbase = happybase.Connection('hbase', port=9090)
    conn_hbase.open()
    conn_hive = hive.Connection(host='hive-metastore', port=10000)
    cursor    = conn_hive.cursor()

    cursor.execute("""
        SELECT ligne_id, vitesse_moy_km_h, nb_vehicules_actifs, etat_trafic
        FROM dakar_smart_city.vue_etat_trafic
    """)

    table = conn_hbase.table(b'dakar:alertes_trafic')
    ts    = datetime.utcnow().isoformat()

    for row in cursor.fetchall():
        ligne, vitesse, nb_v, etat = row
        if etat in ('CRITIQUE', 'MODERE'):
            table.put(ligne.encode(), {
                b'alerte:ligne_id': ligne.encode(),
                b'alerte:vitesse':  str(round(vitesse, 1)).encode(),
                b'alerte:nb_vehic': str(nb_v).encode(),
                b'alerte:etat':     etat.encode(),
                b'alerte:ts':       ts.encode(),
            })

    conn_hbase.close()
    logger.info('HBase mis à jour ✓')


def update_hive_drift(**ctx):
    """T3 — Calcule et persiste le RMSE horaire dans model_drift_history.
    Exécute un INSERT INTO depuis la vue vue_model_drift.
    """
    from pyhive import hive
    conn   = hive.Connection(host='hive-metastore', port=10000)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO dakar_smart_city.model_drift_history
        SELECT periode, current_rmse, 0.0 AS r2_horaire, 15.0
        FROM dakar_smart_city.vue_model_drift
    """)
    conn.close()
    logger.info('Drift historique mis à jour ✓')


# ── Définition du DAG ──────────────────────────────────────────
with DAG(
    'dakar_smart_city_monitoring',
    default_args=default_args,
    description='Monitoring trafic + drift RMSE + réentraînement RF',
    schedule_interval='0 * * * *',   # Chaque heure
    start_date=days_ago(1),
    catchup=False,
    tags=['dakar-smart-city', 'mobilite', 'mlops']
) as dag:

    start = DummyOperator(task_id='start')
    end   = DummyOperator(task_id='end')

    t_drift = BranchPythonOperator(
        task_id='check_model_drift',
        python_callable=check_model_drift,
        provide_context=True
    )
    t_train = PythonOperator(
        task_id='trigger_retrain',
        python_callable=trigger_retrain,
        provide_context=True
    )
    t_hbase = PythonOperator(
        task_id='update_hbase',
        python_callable=update_hbase,
        provide_context=True
    )
    t_hive = PythonOperator(
        task_id='update_hive_drift',
        python_callable=update_hive_drift,
        provide_context=True
    )

    # Flux : start → check drift → [retrain | hbase] → hive → end
    start >> t_drift >> [t_train, t_hbase]
    t_train >> t_hbase >> t_hive >> end
    t_hbase >> t_hive >> end
