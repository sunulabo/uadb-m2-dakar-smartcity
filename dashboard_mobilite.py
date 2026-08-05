# dashboard_mobilite.py — Tableau de bord Dakar Smart City
# 4 panneaux : état trafic, remplissage, BRT vs DDD, drift RMSE

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from pyhive import hive

# ── Connexion Hive ─────────────────────────────────────────────
conn = hive.Connection(
    host='hive-metastore',
    port=10000,
    database='dakar_smart_city'
)

# ── Chargement des données ─────────────────────────────────────
etat_df  = pd.read_sql('SELECT * FROM vue_etat_trafic', conn)
perf_df  = pd.read_sql('SELECT * FROM vue_perf_lignes', conn)
drift_df = pd.read_sql('SELECT * FROM vue_model_drift LIMIT 48', conn)

# ── Dashboard 4 panneaux ───────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle(
    'Dakar Smart City — Dashboard Mobilité Urbaine',
    fontsize=16, fontweight='bold', color='#0D3B6E'
)

# ── Panneau 1 : État trafic par ligne (barplot vitesse) ───────
colors_trafic = [
    '#DC2626' if e == 'CRITIQUE' else
    '#F59E0B' if e == 'MODERE' else '#16A34A'
    for e in etat_df['etat_trafic']
]
axes[0, 0].barh(etat_df['ligne_id'], etat_df['vitesse_moy_km_h'], color=colors_trafic)
axes[0, 0].axvline(x=15, color='orange', linestyle='--', label='Seuil modéré (15 km/h)')
axes[0, 0].axvline(x=5,  color='red',    linestyle='--', label='Seuil critique (5 km/h)')
axes[0, 0].set_xlabel('Vitesse moyenne (km/h)')
axes[0, 0].set_title('État Trafic par Ligne (temps réel)')
axes[0, 0].legend(fontsize=8)

# Légende couleurs
rouge_p  = mpatches.Patch(color='#DC2626', label='CRITIQUE')
orange_p = mpatches.Patch(color='#F59E0B', label='MODERE')
vert_p   = mpatches.Patch(color='#16A34A', label='FLUIDE')
axes[0, 0].legend(handles=[rouge_p, orange_p, vert_p], fontsize=8, loc='lower right')

# ── Panneau 2 : Taux de remplissage par type de véhicule ─────
remplissage = perf_df.groupby('vehicle_type')['taux_remplissage_moy'].mean()
colors_type = ['#1565C0', '#F57C00', '#2E7D32', '#C62828', '#6A1B9A']
axes[0, 1].bar(remplissage.index, remplissage.values,
               color=colors_type[:len(remplissage)])
axes[0, 1].set_xlabel('Type de véhicule')
axes[0, 1].set_ylabel('Passagers moyen')
axes[0, 1].set_title('Taux de Remplissage Moyen par Type')
axes[0, 1].tick_params(axis='x', rotation=30)

# ── Panneau 3 : Performance BRT vs Bus_DDD (30 derniers jours)
for vtype, color in [('BRT', '#1565C0'), ('Bus_DDD', '#F57C00')]:
    sub = perf_df[perf_df['vehicle_type'] == vtype].sort_values('ligne_id')
    if len(sub):
        axes[1, 0].plot(sub['ligne_id'], sub['vitesse_moy'],
                        marker='o', label=vtype, color=color, linewidth=2)
axes[1, 0].set_xlabel('Ligne')
axes[1, 0].set_ylabel('Vitesse moy (km/h)')
axes[1, 0].set_title('Performance BRT vs Bus Dakar Dem Dikk')
axes[1, 0].legend()
axes[1, 0].tick_params(axis='x', rotation=30)
axes[1, 0].axhline(y=15, color='orange', linestyle=':', alpha=0.7)

# ── Panneau 4 : Drift RMSE modèle ────────────────────────────
if len(drift_df) > 0:
    x = range(len(drift_df))
    axes[1, 1].plot(x, drift_df['current_rmse'],
                    marker='o', color='#C62828', linewidth=1.5, label='RMSE horaire')
    axes[1, 1].axhline(y=15, color='orange', linestyle='--',
                       label='Seuil réentraînement (15 km/h)')
    axes[1, 1].fill_between(x, drift_df['current_rmse'], 15,
                             where=drift_df['current_rmse'] > 15,
                             color='red', alpha=0.3, label='Zone drift critique')
else:
    axes[1, 1].text(0.5, 0.5, 'Pas de drift détecté\n(modèle performant)',
                    ha='center', va='center', transform=axes[1, 1].transAxes,
                    fontsize=12, color='green')

axes[1, 1].set_xlabel('Période (heures)')
axes[1, 1].set_ylabel('RMSE (km/h)')
axes[1, 1].set_title('Monitoring Drift Modèle RF')
axes[1, 1].legend(fontsize=8)

# ── Sauvegarde ────────────────────────────────────────────────
plt.tight_layout()
plt.savefig('dashboard_dakar_smart_city.png', dpi=150, bbox_inches='tight')
print('Dashboard sauvegardé : dashboard_dakar_smart_city.png ✓')
