import os
from mysql.connector import pooling

mysqlpool = pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=10,
    pool_reset_session=True,
    host=os.getenv("PROACTIVE_HOST"),
    user=os.getenv("PROACTIVE_USER"),
    password=os.getenv("PROACTIVE_PASSWORD"),
    database=os.getenv("PROACTIVE_DATABASE"),
    port=3306,
)


def query(sql: str, params: None):
    conn = mysqlpool.get_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows
