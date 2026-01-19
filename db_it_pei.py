from __future__ import annotations
from typing import Any, Dict, Optional
from sqlalchemy import create_engine, text
import pandas as pd


def get_engine(secrets):
    url = secrets["postgres"]["url"]
    return create_engine(url, pool_pre_ping=True)


def _clean_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _to_int(x: Any) -> Optional[int]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def fetch_last_by_ue(engine, id_ue: str) -> Optional[Dict[str, Any]]:
    q = text("""
        SELECT *
        FROM it_pei_historial
        WHERE id_ue = :id_ue
        ORDER BY fecha_recepcion DESC NULLS LAST, created_at DESC
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"id_ue": id_ue}).mappings().first()
    return dict(row) if row else None


def insert_it_pei(engine, record: Dict[str, Any]) -> None:
    # obligatorios
    required = ["id_ue", "fecha_recepcion"]
    missing = [k for k in required if not record.get(k)]
    if missing:
        raise ValueError(f"Faltan campos obligatorios: {', '.join(missing)}")

    record = dict(record)
    record["id_ue"] = _clean_str(record.get("id_ue"))
    record["anio"] = _to_int(record.get("anio"))
    record["cantidad_revisiones"] = _to_int(record.get("cantidad_revisiones"))

    cols = list(record.keys())
    col_list = ", ".join(cols)
    param_list = ", ".join([f":{c}" for c in cols])

    q = text(f"""
        INSERT INTO it_pei_historial ({col_list})
        VALUES ({param_list})
    """)

    with engine.begin() as conn:
        conn.execute(q, record)


def search_history(engine, filters: Dict[str, Any], limit: int = 500) -> pd.DataFrame:
    where = []
    params: Dict[str, Any] = {}

    if filters.get("id_ue"):
        where.append("id_ue = :id_ue")
        params["id_ue"] = filters["id_ue"]

    if filters.get("estado"):
        where.append("estado = :estado")
        params["estado"] = filters["estado"]

    if filters.get("tipo_pei"):
        where.append("tipo_pei = :tipo_pei")
        params["tipo_pei"] = filters["tipo_pei"]

    if filters.get("fecha_recepcion_desde"):
        where.append("fecha_recepcion >= :fr_desde")
        params["fr_desde"] = filters["fecha_recepcion_desde"]

    if filters.get("fecha_recepcion_hasta"):
        where.append("fecha_recepcion <= :fr_hasta")
        params["fr_hasta"] = filters["fecha_recepcion_hasta"]

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    params["limit"] = limit

    q = text(f"""
        SELECT
          id, id_ue,
          anio, ng1, ng2, fecha_recepcion, periodo_pei, vigencia, tipo_pei, estado,
          responsable_institucional, cantidad_revisiones, fecha_derivacion, etapas_revision,
          comentario_adicional_emisor_it, articulacion, expediente, fecha_it, numero_it,
          fecha_oficio, numero_oficio,
          created_at, created_by
        FROM it_pei_historial
        {where_sql}
        ORDER BY fecha_recepcion DESC NULLS LAST, created_at DESC
        LIMIT :limit
    """)

    with engine.begin() as conn:
        df = pd.read_sql(q, conn, params=params)

    return df
