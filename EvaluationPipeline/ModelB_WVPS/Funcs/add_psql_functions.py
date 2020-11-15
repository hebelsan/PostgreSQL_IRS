import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql

from .config import config
import os

##
#   helper function for db connection
##
def get_connection():
    params = config()
    # connect to the PostgreSQL server
    #print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


def add_function(file, cur):
    sql_string = ""
    is_function = False
    for line in open(file, 'r'):
        split = line.split(' ')
        if split[0] == 'CREATE':
            is_function = True
            sql_string += line
        elif split[0] == '$$' or split[0] == ');':
            is_function = False
            sql_string += line
            cur.execute(sql_string)
            sql_string = ""
        elif is_function:
            sql_string += line


def add_psql_functions():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        add_function(os.path.realpath(__file__)[:-21] + "/PSQL_functions/functions.txt", cur)
        add_function(os.path.realpath(__file__)[:-21] + "/PSQL_functions/helpFuncs.txt", cur)
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    
