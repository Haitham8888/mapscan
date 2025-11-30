# db/db2.py
import os

try:
    import jaydebeapi
    import jpype
    JDBC_AVAILABLE = True
except Exception:
    JDBC_AVAILABLE = False

from config import Config


def get_db2_connection():
    """
    فتح اتصال DB2 عن طريق jaydebeapi.
    تأكد إن مسار jar مضبوط عندك.
    """
    if not JDBC_AVAILABLE:
        raise RuntimeError("jaydebeapi/jpype غير متوفرين، فعّلهم أول.")

    driver_class = Config.DB2_DRIVER_CLASS
    url = Config.DB2_JDBC_URL
    user = Config.DB2_USER
    password = Config.DB2_PASSWORD

    # عدّل المسارات حسب جهازك
    jars = [
        "/opt/ibm/driver/db2jcc4.jar",
        "/opt/ibm/driver/db2jcc_license_cu.jar",
    ]

    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=":".join(jars))

    conn = jaydebeapi.connect(driver_class, url, [user, password])
    return conn
