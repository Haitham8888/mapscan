# config.py
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


class Config:
    # مسارات ملفات GeoJSON
    REGIONS_GEOJSON_PATH = os.path.join(BASE_DIR, "static", "geojson", "regions.geojson")
    CITIES_GEOJSON_PATH = os.path.join(BASE_DIR, "static", "geojson", "cities.geojson")
    DISTRICTS_GEOJSON_PATH = os.path.join(BASE_DIR, "static", "geojson", "districts.geojson")

    # حقول الأسماء داخل GeoJSON (عدّلها لو عندك أسماء مختلفة)
    REGIONS_NAME_FIELD = "name_ar"
    CITIES_NAME_FIELD = "name_ar"
    DISTRICTS_NAME_FIELD = "name_ar"

    # إعدادات DB2 (اسحب عدد المواطنين فقط)
    DB2_DRIVER_CLASS = "com.ibm.db2.jcc.DB2Driver"
    DB2_JDBC_URL = "jdbc:db2://your-db-host:50000/YOUR_DB"
    DB2_USER = "your_user"
    DB2_PASSWORD = "your_password"

    # جدول السكان
    POPULATION_TABLE = "EDWH.POPULATION_TABLE"
    DB2_JARS = [
        "/path/to/db2jcc4.jar",
        "/path/to/db2jcc4_license_cu.jar"
    ]