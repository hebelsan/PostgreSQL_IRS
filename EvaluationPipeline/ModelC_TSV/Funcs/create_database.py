import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql

from .config import config

from math import sqrt
import re


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
#   helper function for db_create_tables()
##
def create_tables(conn, cur):
    cur.execute("CREATE TABLE " + TABLE_DOCUMENT + " ( \
                    Id SERIAL PRIMARY KEY, \
                    Title TEXT, \
                    NumSections INTEGER);")
    
    cur.execute("CREATE TABLE " + TABLE_SECTION + " ( \
                    Id SERIAL PRIMARY KEY, \
                    Tsvector TSVECTOR, \
                    DocID INTEGER REFERENCES " + TABLE_DOCUMENT + " (Id), \
                    NumWords INTEGER, \
                    Weight REAL);")
    
    # idf materialized view
    cur.execute("CREATE MATERIALIZED VIEW idfview AS \
                    SELECT word as Term, ndoc as Df, \
                    cast(log(cast(NumSections AS REAL)/ndoc) AS REAL) AS IDF, \
                    cast(log(1+ ((NumSections - ndoc + 0.5) / (ndoc + 0.5))) AS REAL) AS BM25IDF \
                    FROM ts_stat('SELECT tsvector FROM Section'), \
                    (select count(*) as NumSections from Section) as N;")
    
    
    conn.commit()
    print("succefully created tables!")


def add_indices(cur):
    cur.execute("CREATE INDEX tsv_idx ON " + TABLE_SECTION + " USING GIN (Tsvector);")


def db_create_init_file(database_name, user_name):
    config_file_name = "database.ini"
    if path.exists(config_file_name):
        print("file " + config_file_name + " already exists")
    else:
        f = open(config_file_name,"w+")
        f.write("[postgresql]\n")
        f.write("database=" + database_name + "\n")
        f.write("user=" + user_name)


##
#   creates the tables and functions
##
def db_create_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        create_tables(conn, cur)
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def db_insert_single_document(doc_title, text, cur):
    sections = text.split('\n\n')
    
    # DOCUMENT
    sql_string = "INSERT INTO " + TABLE_DOCUMENT + " \
                        (Title, NumSections) \
                    VALUES \
                        (%s, %s) \
                    RETURNING Id;"
    cur.execute(sql_string, (doc_title, len(sections)))
    doc_id = cur.fetchone()[0]
    
    for sec in sections:
        if sec == "":
            continue
        
        num_words = len(sec.split(' '))
        # SECTION
        sql_string = "INSERT INTO " + TABLE_SECTION + " \
                            (DocID, tsvector, NumWords, Weight) \
                        VALUES \
                            (%s, \
                            to_tsvector('english', COALESCE(%s,'')), \
                            %s, %s);"
        cur.execute(sql_string, (doc_id, sec, num_words, 1/sqrt(num_words)))


# % param: with_vocab: bool identifieny if doc entries should also be remove from Vocab
def db_delete_document(doc_title):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # get the doc id
        sql_string = "SELECT Id FROM " + TABLE_DOCUMENT + " WHERE title = %s"
        cur.execute(sql_string, (doc_title, ))
        doc_id = cur.fetchone()
        if not (doc_id):
            cur.close()
            print("no document with this title found...")
            return
        
        doc_id = doc_id[0]
        
        # get section ids
        sql_string = "SELECT Id FROM " + TABLE_SECTION + " WHERE docid = %s"
        cur.execute(sql_string, (doc_id, ))
        section_ids = cur.fetchall()
        if not (section_ids):
            cur.close()
            print("the document has no sections...")
            return
        # unpack sections
        section_ids = [sec[0] for sec in section_ids]
        
        # delete sections
        sql_string = "DELETE FROM " + TABLE_SECTION + " WHERE docid = %s;"
        cur.execute(sql_string, (doc_id, ))
        
        # delete document
        sql_string = "DELETE FROM " + TABLE_DOCUMENT + " WHERE id = %s;"
        cur.execute(sql_string, (doc_id, ))
        
        conn.commit()
        cur.close()
        print("Deletion was succesful")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def db_show_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        print(TABLE_DOCUMENT + ":")
        cur.execute("SELECT * FROM "+ TABLE_DOCUMENT + ";")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        
        print(TABLE_SECTION + ":")
        cur.execute("SELECT * FROM "+ TABLE_SECTION + ";")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        
        print("idfview:")
        cur.execute("SELECT * FROM idfview;")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def db_show_num_entries():
    print("ENTRIES:")
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT count(*) FROM "+ TABLE_SECTION + ";")
        print("Sections: " + str(cur.fetchone()[0]))
        
        cur.execute("SELECT count(*) FROM "+ TABLE_DOCUMENT + ";")
        print("Documents: " + str(cur.fetchone()[0]))
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def db_show_memory_usage():
    print("MEMORY SIZE:")
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        for table_name in TABLES:
            cur.execute("SELECT pg_size_pretty( pg_total_relation_size(%s) );", (table_name,))
            print(table_name + ": " + cur.fetchone()[0])

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


##
#   deletes all tables and rules
##
def db_drop_all_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("DROP MATERIALIZED VIEW idfview;")
        
        for table_name in TABLES:
            cur.execute("DROP TABLE " + table_name + " CASCADE;")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


def db_insert_test_data():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        TEST_DOC = "find and result in the first paragraph\n\n The next paragraph has find in it\n\n find the last has again both at the beginning and end result result find"
        db_insert_single_document("Test Title", TEST_DOC, cur)
        
        cur.execute("REFRESH MATERIALIZED VIEW idfview;")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def db_insert_dataset(df):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        df.apply(lambda x: db_insert_single_document(x['title'], x['doc'], cur), axis=1)
        
        cur.execute("REFRESH MATERIALIZED VIEW idfview;")
        
        add_indices(cur)
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_num_sections():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT count(*) FROM "+ TABLE_SECTION + ";")
        num_sections = cur.fetchone()[0]
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return num_sections


if __name__ == "__main__":
    db_drop_all_tables()
    db_create_tables()
    
    db_insert_test_data()
    db_show_tables()
    
    #db_delete_document("Test Title", True)
    #db_show_tables()
    
    
    
