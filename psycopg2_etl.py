import psycopg2
import os

# Connection parametrs
conn_string_source= "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
conn_string_target= "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'" 

with psycopg2.connect(conn_string_source) as conn, conn.cursor() as cursor:
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    cursor.execute(query)
    result = cursor.fetchall()


for element, in result:
    query_ddl = os.popen(f"docker exec -it my_postgres pg_dump -t {element} --schema-only my_database").read()
    rows_ddl = query_ddl.split('\n')
    rows_ddl.remove(rows_ddl[20])
    query_ddl = '\n'.join(rows_ddl)
    with psycopg2.connect(conn_string_target) as conn, conn.cursor() as cursor:
        cursor.execute(f'drop table if exists {element}')
        a = cursor.execute(query_ddl)

with psycopg2.connect(conn_string_target) as conn, conn.cursor() as cursor:
    query = "SELECT table_name FROM information_schema.tables where table_schema like 'public';"
    cursor.execute(query)
    result = cursor.fetchall()
    for element, in result:
        q = f"COPY {element} from STDIN WITH DELIMITER ',' CSV HEADER;"
        with open(f'/home/user1/ETL/L5/{element}.csv', 'r') as f:
            cursor.copy_expert(q, f)

