import psycopg2


# 获取数据库连接
def get_conn(Host='bigdata', DataBase='dbd001', Port='5432', UserName='myetl', Password='Etl@qe1230o'):
    return psycopg2.connect(host=Host, user=UserName, password=Password, database=DataBase, port=Port)


# 获取cursor
def get_cursor(conn):
    return conn.cursor()


# 关闭连接
def conn_close(conn):
    if conn is not None:
        conn.close()


# 关闭cursor
def cursor_close(cursor):
    if cursor is not None:
        cursor.close()


# 关闭所有
def close(cursor, conn):
    cursor_close(cursor)
    conn_close(conn)
