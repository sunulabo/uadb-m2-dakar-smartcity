# kafka_producer_dakar.py — Simulateur GPS véhicules Dakar
# BRT, Dakar Dem Dikk, Taxis, Clandos — avec saisonnalité heure de pointe

from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
import numpy as np

random.seed(42)
np.random.seed(42)

# Lignes de transport Dakar avec parcours réels (approximatif)
LIGNES = {
    'BRT-01':  {'type': 'BRT',        'nb_vehicules': 12, 'depart': (14.693, -17.447), 'arrivee': (14.772, -17.347)},
    'DDD-15':  {'type': 'Bus_DDD',    'nb_vehicules': 20, 'depart': (14.680, -17.440), 'arrivee': (14.730, -17.400)},
    'DDD-22':  {'type': 'Bus_DDD',    'nb_vehicules': 15, 'depart': (14.693, -17.447), 'arrivee': (14.760, -17.390)},
    'TAXI-PL': {'type': 'Taxi',       'nb_vehicules': 50, 'depart': (14.693, -17.447), 'arrivee': (14.750, -17.380)},
    'CLAN-01': {'type': 'Clandos',    'nb_vehicules': 30, 'depart': (14.710, -17.430), 'arrivee': (14.775, -17.360)},
    'CAR-GD':  {'type': 'Car_Rapide', 'nb_vehicules': 10, 'depart': (14.680, -17.450), 'arrivee': (14.760, -17.370)},
}

# Zones de congestion connues (Plateau, Colobane, VDN, Guédiawaye)
ZONES_CONGESTION = [
    (14.693, -17.447),  # Plateau / Centre-ville
    (14.720, -17.430),  # Colobane
    (14.744, -17.410),  # VDN Pikine
    (14.760, -17.395),  # Guédiawaye
]

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode()
)


def near_congestion(lat: float, lon: float) -> bool:
    """Vérifie si le véhicule est dans un rayon de ~500m d'une zone de congestion."""
    for clat, clon in ZONES_CONGESTION:
        dist = ((lat - clat)**2 + (lon - clon)**2) ** 0.5
        if dist < 0.005:  # ~500m en degrés
            return True
    return False


def gen_gps_event(ligne_id: str) -> dict:
    """Génère un événement GPS réaliste pour un véhicule d'une ligne."""
    ligne = LIGNES[ligne_id]
    h = datetime.utcnow().hour

    # Heures de pointe : 7-9h et 17-20h → vitesse réduite, plus de passagers
    heure_pointe = (7 <= h <= 9) or (17 <= h <= 20)
    speed_base = 8 if heure_pointe else 25   # km/h
    pass_base  = 55 if heure_pointe else 25

    # Position interpolée entre départ et arrivée
    t = random.random()
    lat = ligne['depart'][0] + t * (ligne['arrivee'][0] - ligne['depart'][0])
    lon = ligne['depart'][1] + t * (ligne['arrivee'][1] - ligne['depart'][1])

    # Bruit GPS réaliste (±50m)
    lat += np.random.normal(0, 0.0005)
    lon += np.random.normal(0, 0.0005)

    # Congestion locale → vitesse encore réduite
    if near_congestion(lat, lon):
        speed_base = max(2, speed_base * 0.4)

    # Génération d'une plaque sénégalaise valide (format XX-9999-X)
    prefixes = ['DK', 'TH', 'KL']
    suffixes = list('ABCDE')
    vehicle_id = f'{random.choice(prefixes)}-{random.randint(1000, 9999)}-{random.choice(suffixes)}'

    return {
        'vehicle_id':   vehicle_id,
        'vehicle_type': ligne['type'],
        'latitude':     round(max(14.60, min(14.85, lat)), 6),
        'longitude':    round(max(-17.55, min(-17.30, lon)), 6),
        'speed_kmh':    round(max(0, np.random.normal(speed_base, 3)), 1),
        'heading_deg':  round(random.uniform(0, 360), 1),
        'ligne_id':     ligne_id,
        'passagers':    min(100, max(0, int(np.random.normal(pass_base, 10)))),
        'timestamp':    datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
    }


if __name__ == '__main__':
    print('Simulateur Dakar Smart City démarré...')
    while True:
        for ligne_id in LIGNES:
            event = gen_gps_event(ligne_id)
            producer.send('dakar_mobility_clean', event)
            print(f"[{event['timestamp']}] {ligne_id} | {event['vehicle_type']} | {event['speed_kmh']} km/h")
        producer.flush()
        time.sleep(2)
