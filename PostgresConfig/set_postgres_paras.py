import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql


def set_postgresql_paras_new():
    try:
        conn = psycopg2.connect(dbname="", user="postgres", password="")
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("ALTER SYSTEM SET max_connections = '100';") # 100
        cur.execute("ALTER SYSTEM SET shared_buffers = '4GB';") # 128MB
        cur.execute("ALTER SYSTEM SET effective_cache_size = '12GB';") # 4GB
        cur.execute("ALTER SYSTEM SET work_mem = '256MB';") # 4MB
        cur.execute("ALTER SYSTEM SET maintenance_work_mem = '1GB';") # 64MB
        cur.execute("ALTER SYSTEM SET min_wal_size = '1GB';") # 80MB
        cur.execute("ALTER SYSTEM SET max_wal_size = '2GB';") # 1GB
        
        cur.execute("ALTER SYSTEM SET checkpoint_completion_target = '0.5';") # typically 16MB
        cur.execute("ALTER SYSTEM SET wal_buffers = '16MB';") # typically 16MB
        cur.execute("ALTER SYSTEM SET default_statistics_target = '100';") # default 100
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def set_postgresql_paras_old():
    try:
        conn = psycopg2.connect(dbname="", user="postgres", password="")
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("ALTER SYSTEM SET max_connections = '100';") # 100
        cur.execute("ALTER SYSTEM SET shared_buffers = '128MB';") # 128MB
        cur.execute("ALTER SYSTEM SET effective_cache_size = '4GB';") # 4GB
        cur.execute("ALTER SYSTEM SET work_mem = '4MB';") # 4MB
        cur.execute("ALTER SYSTEM SET maintenance_work_mem = '64MB';") # 64MB
        cur.execute("ALTER SYSTEM SET min_wal_size = '80MB';") # 80MB
        cur.execute("ALTER SYSTEM SET max_wal_size = '1GB';") # 1GB
        
        cur.execute("ALTER SYSTEM SET checkpoint_completion_target = '0.5';") # 0.5
        cur.execute("ALTER SYSTEM SET wal_buffers = '16MB';") # typically 16MB
        cur.execute("ALTER SYSTEM SET default_statistics_target = '100';") # default 100
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")
            



if __name__ == "__main__":
    print('0: new parameter (recommended)')
    print('1: old parameter')
    num = input('')
    if num == '0':
        set_postgresql_paras_new()
    elif num == '1':
        set_postgresql_paras_old()
    print('want to create necessaray databases? y/n ')
    create = input('')
    if create == 'y' or create == 'Y':
        print('username: ')
        username = input('')
        print('database: ')
        database = input('')
        try:
            conn = psycopg2.connect(
                database="mydb",
                user="alex")
            cur = conn.cursor()
            sql_query = "CREATE DATABASE modela";
            cur.execute(sql_query)
            sql_query = "CREATE DATABASE modelb";
            cur.execute(sql_query)
            sql_query = "CREATE DATABASE tsvector";
            cur.execute(sql_query)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

