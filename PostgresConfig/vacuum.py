import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql


def vacuum_full_moda():
    try:
        conn = psycopg2.connect(dbname="modela", user="postgres", password="")
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("VACUUM FULL;")
        cur.execute("ANALYSE;")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def vacuum_full_modb():
    try:
        conn = psycopg2.connect(dbname="modelb", user="postgres", password="")
        conn.autocommit = True
        cur = conn.cursor()
        
        cur.execute("VACUUM FULL;")
        cur.execute("ANALYSE;")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    print('vacum mod a y/n :')
    res = input('')
    if res == 'y':
        print("start...")
        vacuum_full_moda()
        print("model a vacuumed")
    print('vacum mod b y/n :')
    res = input('')
    if res == 'y':
        vacuum_full_modb()
        print("model b vacuumed")
    

