import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql

from .config import config

TABLE_DOCUMENT = "Document"
TABLE_SECTION = "Section"

TABLES = [TABLE_DOCUMENT, TABLE_SECTION]

##
#   helper function for db connection
##
def get_connection():
    params = config()
    # connect to the PostgreSQL server
    #print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


##
#  search
##
def db_search(query, analyse=False):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_string = "SELECT Id \
                      FROM " + TABLE_SECTION + ", \
                        to_tsquery('english', %s) query \
                      WHERE query @@ Tsvector;"
        if analyse:
            sql_string = "EXPLAIN ANALYSE " + sql_string
        cur.execute(sql_string, (query, ))
        res = cur.fetchall()
        section_ids = [sec[0] for sec in res]
        
        #if num_results < 20:
            #print(section_ids)
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return section_ids


def get_num_results(query):
    result_ids = db_search(query)
    return len(result_ids)
