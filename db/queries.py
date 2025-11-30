from db.db2 import get_db2_connection
from config import POPULATION_TABLE


def fetch_population(region_id=None, city_id=None, district_id=None):
    conn = get_db2_connection()
    cur = conn.cursor()

    try:
        if district_id:
            sql = f"""SELECT POP_M, POP_F, POP_TOTAL 
                      FROM {POPULATION_TABLE}
                      WHERE DISTRICT_ID = ?
                      FETCH FIRST 1 ROWS ONLY"""
            params = [district_id]

        elif city_id:
            sql = f"""SELECT POP_M, POP_F, POP_TOTAL 
                      FROM {POPULATION_TABLE}
                      WHERE CITY_ID = ?
                      FETCH FIRST 1 ROWS ONLY"""
            params = [city_id]

        elif region_id:
            sql = f"""SELECT POP_M, POP_F, POP_TOTAL 
                      FROM {POPULATION_TABLE}
                      WHERE REGION_ID = ?
                      FETCH FIRST 1 ROWS ONLY"""
            params = [region_id]
        else:
            return None

        cur.execute(sql, params)
        row = cur.fetchone()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    if not row:
        return None

    # row may be tuple-like
    try:
        return {
            "population_male": float(row[0]) if row[0] is not None else 0.0,
            "population_female": float(row[1]) if row[1] is not None else 0.0,
            "population_total": float(row[2]) if row[2] is not None else 0.0,
        }
    except Exception:
        return None
