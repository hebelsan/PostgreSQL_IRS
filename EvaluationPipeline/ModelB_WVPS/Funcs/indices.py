import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
import math

from .config import config
from .search import *

TABLE_VOCAB = "Vocabulary"
TABLE_WORD_STEM = "WordStem"
TABLE_DOCUMENT = "Document"
TABLE_SECTION = "Section"

TABLES = [TABLE_VOCAB, TABLE_WORD_STEM, TABLE_DOCUMENT, TABLE_SECTION]


##
#   helper function for db connection
##
def get_connectionb():
    params = config()
    # connect to the PostgreSQL server
    #print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


def execute_statementb(statement):
    conn = None
    try:
        conn = get_connectionb()
        cur = conn.cursor()
        
        cur.execute(statement)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def vacuumb():
    conn = None
    try:
        conn = get_connectionb()
        cur = conn.cursor()
        
        conn.autocommit = True
        for table in TABLES:
            cur.execute("VACUUM " + table + ";")
        for table in TABLES:
            cur.execute("ANALYSE " + table + ";")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


#SELECT indexname FROM pg_indexes WHERE tablename = ANY(ARRAY['vocabulary', 'wordstem', 'document', 'section', 'idfview'])
#AND indexname NOT LIKE '%_pkey%' AND indexname NOT LIKE 'pg_%';

def remove_allb():
    sql_string_create_func = "CREATE OR REPLACE FUNCTION drop_all_indexes()\
                             RETURNS INTEGER AS $$ \
                  DECLARE \
                    tb RECORD; \
                  BEGIN \
                    FOR tb IN (SELECT indexname \
                            FROM pg_indexes WHERE \
                            tablename = ANY(ARRAY['vocabulary', 'wordstem', 'document', 'section', 'section_old', 'sectionterm', 'idfview']) \
                                AND indexname NOT LIKE '%_pkey%' \
                                AND indexname NOT LIKE 'pg_%' \
                                AND indexname NOT LIKE '%_key' \
                            ) LOOP \
                      EXECUTE 'DROP INDEX ' || tb.indexname; \
                    END LOOP; \
                  RETURN 1; \
                  END; \
                  $$ LANGUAGE plpgsql;"
    execute_statementb(sql_string_create_func)
    execute_statementb("SELECT drop_all_indexes();")


def modelB_add_standard_slow():
    remove_allb()
    execute_statementb("CREATE INDEX stem_indx ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statementb("CREATE INDEX voc_indx ON " + TABLE_VOCAB + "(Term);")
    execute_statementb("CREATE INDEX doc_indx ON " + TABLE_DOCUMENT + "(Id);")
    execute_statementb("CREATE INDEX sec_indx_id ON " + TABLE_SECTION + "(Id);")
    execute_statementb("CREATE INDEX idf_indx ON IdfView (Termid);")
    vacuumb()


def modelB_add_standard():
    remove_allb()
    execute_statementb("CREATE INDEX stem_indx ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statementb("CREATE INDEX voc_indx ON " + TABLE_VOCAB + "(Term);")
    execute_statementb("CREATE INDEX doc_indx ON " + TABLE_DOCUMENT + "(Id);")
    execute_statementb("CREATE INDEX sec_indx_id ON " + TABLE_SECTION + "(Id);")
    execute_statementb("CREATE INDEX idf_indx ON IdfView (Termid);")
    execute_statementb("CREATE INDEX search_indx ON " + TABLE_SECTION + " USING GIN (Stems gin__int_ops);")
    vacuumb()


def modelB_add_gist_normal():
    remove_allb()
    execute_statementb("CREATE INDEX stem_indx ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statementb("CREATE INDEX voc_indx ON " + TABLE_VOCAB + "(Term);")
    execute_statementb("CREATE INDEX doc_indx ON " + TABLE_DOCUMENT + "(Id) include (Title);")
    execute_statementb("CREATE INDEX sec_indx_w ON " + TABLE_SECTION + "(Id) include (NumWords);")
    execute_statementb("CREATE INDEX sec_indx_d ON " + TABLE_SECTION + "(Id) include (DocId);")
    execute_statementb("CREATE INDEX idf_indx ON IdfView (Termid);")
    execute_statementb("CREATE INDEX search_indx ON " + TABLE_SECTION + " USING GIST (Stems gist__int_ops);")
    vacuumb()


def modelB_add_gist_big():
    remove_allb()
    execute_statementb("CREATE INDEX stem_indx ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statementb("CREATE INDEX voc_indx ON " + TABLE_VOCAB + "(Term);")
    execute_statementb("CREATE INDEX doc_indx ON " + TABLE_DOCUMENT + "(Id) include (Title);")
    execute_statementb("CREATE INDEX sec_indx_w ON " + TABLE_SECTION + "(Id) include (NumWords);")
    execute_statementb("CREATE INDEX sec_indx_d ON " + TABLE_SECTION + "(Id) include (DocId);")
    execute_statementb("CREATE INDEX idf_indx ON IdfView (Termid);")
    execute_statementb("CREATE INDEX search_indx ON " + TABLE_SECTION + " USING GIST (Stems gist__intbig_ops);")
    vacuumb()

def modelB_add_indx_only():
    remove_allb()
    execute_statementb("CREATE INDEX stem_indx ON " + TABLE_WORD_STEM + "(Stem) INCLUDE (Id);")
    execute_statementb("CREATE INDEX voc_indx ON " + TABLE_VOCAB + "(Term) INCLUDE (Id);")
    execute_statementb("CREATE INDEX doc_indx ON " + TABLE_DOCUMENT + "(Id) INCLUDE (Title);")
    execute_statementb("CREATE INDEX sec_indx_w ON " + TABLE_SECTION + "(Id) INCLUDE (NumWords);")
    execute_statementb("CREATE INDEX sec_indx_d ON " + TABLE_SECTION + "(Id) INCLUDE (DocId);")
    execute_statementb("CREATE INDEX idf_indx1 ON IdfView (Termid) INCLUDE (IDF);")
    execute_statementb("CREATE INDEX idf_indx2 ON IdfView (Termid) INCLUDE (BM25IDF);")
    execute_statementb("CREATE INDEX search_indx ON " + TABLE_SECTION + " USING GIN (Stems gin__int_ops);")
    vacuumb()


if __name__ == "__main__":
    pass
