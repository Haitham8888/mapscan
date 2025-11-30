# db/queries.py
from typing import Optional
from config import Config
from .db2 import get_db2_connection


def get_population(region_id: Optional[int] = None,
                   city_id: Optional[int] = None,
                   district_id: Optional[int] = None) -> Optional[int]:
    """
    يرجع مجموع POP_TOTAL حسب الفلاتر المعطاة.
    إذا الكل None يرجّع مجموع المملكة كاملة.
    """
    table = Config.POPULATION_TABLE
    where_clauses = []
    params = []

    if region_id is not None:
        where_clauses.append("REGION_ID = ?")
        params.append(region_id)
    if city_id is not None:
        where_clauses.append("CITY_ID = ?")
        params.append(city_id)
    if district_id is not None:
        where_clauses.append("DISTRICT_ID = ?")
        params.append(district_id)

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    sql = f"SELECT SUM(POP_TOTAL) AS total_pop FROM {table}{where_sql}"

    conn = get_db2_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        if row and row[0] is not None:
            return int(row[0])
        return None
    finally:
        conn.close()
