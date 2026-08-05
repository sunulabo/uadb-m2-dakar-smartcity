# schema.py — Contrat de données Dakar Smart City
# Validation Pandera : coordonnées GPS Dakar + plaques sénégalaises

import pandera as pa
import pandas as pd
from pandera.typing import Series

# Types de véhicules Dakar (CETUD / Dakar Dem Dikk)
VEHICLE_TYPES = ['Bus_DDD', 'Taxi', 'BRT', 'Clandos', 'Car_Rapide']


class DakarMobilitySchema(pa.SchemaModel):
    """Validation stricte des données GPS véhicules Dakar."""

    # Format plaque sénégalais : ex. DK-1234-A
    vehicle_id:   Series[str]   = pa.Field(str_matches=r'^[A-Z]{2}-\d{4}-[A-Z]$')
    vehicle_type: Series[str]   = pa.Field(isin=VEHICLE_TYPES)

    # Bounding box Grand Dakar (Cap-Vert péninsule + banlieue)
    latitude:     Series[float] = pa.Field(ge=14.60, le=14.85)
    longitude:    Series[float] = pa.Field(ge=-17.55, le=-17.30)
    speed_kmh:    Series[float] = pa.Field(ge=0.0, le=120.0)
    heading_deg:  Series[float] = pa.Field(ge=0.0, le=360.0)
    ligne_id:     Series[str]   = pa.Field()       # ex. 'BRT-01', 'DDD-15'
    passagers:    Series[int]   = pa.Field(ge=0, le=100)
    timestamp:    Series[pd.Timestamp] = pa.Field(nullable=False)

    class Config:
        coerce = True
        strict = True


def validate_and_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Valide et filtre les enregistrements GPS invalides."""
    try:
        return DakarMobilitySchema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        err = exc.failure_cases
        import logging
        logging.warning(f'[DakarMobility] {len(err)} GPS invalide(s) rejeté(s)')
        valid_idx = df.index.difference(err['index'].dropna().astype(int))
        return df.loc[valid_idx]
