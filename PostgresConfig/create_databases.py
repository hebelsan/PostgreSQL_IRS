import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql


if __name__ == "__main__":
    print('want to create necessaray databases? y/n ')
    create = input('')
    if create == 'y' or create == 'Y':
        print('username: ')
        username = input('')
        print('database: ')
        database = input('')
        try:
            conn = psycopg2.connect(database=database, user=username)
            cur = conn.cursor()
            conn.autocommit = True
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

