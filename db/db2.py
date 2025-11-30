import jpype
import jaydebeapi
from config import DB2_JARS, DB2_JDBC_URL, DB2_USERNAME, DB2_PASSWORD
import os


def start_jvm():
    if not jpype.isJVMStarted():
        # DB2_JARS may be a list of jar paths
        if isinstance(DB2_JARS, (list, tuple)):
            classpath = DB2_JARS
        elif isinstance(DB2_JARS, str):
            classpath = DB2_JARS.split(os.pathsep)
        else:
            classpath = None

        if classpath:
            jpype.startJVM(classpath=classpath)
        else:
            jpype.startJVM()


def get_db2_connection():
    start_jvm()
    # jaydebeapi accepts jars param; pass DB2_JARS as list if available
    jars = DB2_JARS if isinstance(DB2_JARS, (list, tuple)) else None
    return jaydebeapi.connect(
        "com.ibm.db2.jcc.DB2Driver",
        DB2_JDBC_URL,
        [DB2_USERNAME, DB2_PASSWORD],
        jars=jars,
    )
