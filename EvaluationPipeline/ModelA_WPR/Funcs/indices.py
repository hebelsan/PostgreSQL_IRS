import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
import math

from .config import config
from .search import *

TABLE_VOCAB = "Vocabulary"
TABLE_WORD_STEM = "WortStem"
TABLE_DOCUMENT = "Document"
TABLE_SECTION = "Section"
TABLE_SECTION_TERM = "SectionTerm"

TABLES = [TABLE_VOCAB, TABLE_WORD_STEM, TABLE_DOCUMENT,
            TABLE_SECTION, TABLE_SECTION_TERM]


## CLUSTER
# 
#   DROP INDEX section_term_index;
#   CREATE INDEX section_term_index ON sectionterm (StemId)
#   CLUSTER sectionterm USING section_term_index;
#   CREATE INDEX section_term_index ON sectionterm USING BRIN (StemId);
# COMMIT;

##
#   helper function for db connection
##
def get_connection():
    params = config()
    # connect to the PostgreSQL server
    #print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


def execute_statement(statement):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute(statement)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def order_stems():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        conn.autocommit = True
        for table in TABLES:
            cur.execute("VACUUM " + table + ";")
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def vacuum():
    conn = None
    try:
        conn = get_connection()
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

'''
def remove_all():
    sql_string_create_func = "CREATE OR REPLACE FUNCTION drop_all_indexes()\
                             RETURNS INTEGER AS $$ \
                  DECLARE \
                    i RECORD; \
                  BEGIN \
                    FOR i IN \
                    (SELECT relname FROM pg_class \
                    WHERE relkind = 'i' AND relname NOT LIKE '%_pkey%' \
                                        AND relname NOT LIKE 'pg_%' \
                                        AND relname NOT LIKE '%_key' \
                                        AND relname NOT LIKE '%_part%') LOOP \
                      EXECUTE 'DROP INDEX ' || i.relname; \
                    END LOOP; \
                  RETURN 1; \
                  END; \
                  $$ LANGUAGE plpgsql;"
    execute_statement(sql_string_create_func)
    execute_statement("SELECT drop_all_indexes();")
'''

#SELECT indexname FROM pg_indexes WHERE tablename = ANY(ARRAY['vocabulary', 'wortstem', 'document', 'section', 'sectionterm', 'idfview']) 
#AND indexname NOT LIKE '%_pkey%' AND indexname NOT LIKE 'pg_%';

def remove_all():
    sql_string_create_func = "CREATE OR REPLACE FUNCTION drop_all_indexes()\
                             RETURNS INTEGER AS $$ \
                  DECLARE \
                    tb RECORD; \
                  BEGIN \
                    FOR tb IN (SELECT indexname \
                            FROM pg_indexes WHERE \
                            tablename = ANY(ARRAY['vocabulary', 'wortstem', 'document', 'section', 'sectionterm', 'sectionterm_old', 'idfview']) \
                                AND indexname NOT LIKE '%_pkey%' \
                                AND indexname NOT LIKE 'pg_%' \
                                AND indexname NOT LIKE '%_key' \
                            ) LOOP \
                      EXECUTE 'DROP INDEX ' || tb.indexname; \
                    END LOOP; \
                  RETURN 1; \
                  END; \
                  $$ LANGUAGE plpgsql;"
    execute_statement(sql_string_create_func)
    execute_statement("SELECT drop_all_indexes();")


def modelA_add_standard_slow():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    vacuum()


def modelA_add_standard():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index ON " + TABLE_SECTION_TERM + "(StemID);")
    vacuum()


def modelA_add_indx_only():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem) INCLUDE (Id);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index ON " + TABLE_SECTION_TERM + "(StemId) INCLUDE (SectionID);")
    vacuum()



def modelA_add_indx_only_all():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem) INCLUDE (Id);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index ON " + TABLE_SECTION_TERM + "(StemId) INCLUDE (SectionID);")
    execute_statement("CREATE INDEX section_term_index2 ON " + TABLE_SECTION_TERM + "(StemId) INCLUDE (SectionID, tf);")
    vacuum()


def modelA_add_multi():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index_grouped ON " + TABLE_SECTION_TERM + "(StemID, SectionId);")
    vacuum()


def modelA_add_multi_all():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index_grouped ON " + TABLE_SECTION_TERM + "(StemID, SectionId);")
    execute_statement("CREATE INDEX section_term_index_grouped2 ON " + TABLE_SECTION_TERM + "(StemID, SectionId, tf);")
    vacuum()


def modelA_add_index_hash():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index ON " + TABLE_SECTION_TERM + " USING hash (StemID);")
    vacuum()


def modelA_add_index_brin():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index ON " + TABLE_SECTION_TERM + " USING BRIN (StemID)")
    vacuum()


def modelA_add_index_brin_32():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index ON " + TABLE_SECTION_TERM + " USING BRIN (StemID) WITH (pages_per_range = 32);")
    vacuum()


def modelA_add_index_brin_16():
    remove_all()
    execute_statement("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem);")
    execute_statement("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term);")
    execute_statement("CREATE INDEX doc_index ON " + TABLE_DOCUMENT + "(Title);")
    execute_statement("CREATE INDEX num_words_index ON " + TABLE_SECTION + "(NumWords);")
    execute_statement("CREATE INDEX doc_id_index ON " + TABLE_SECTION + "(DocId);")
    execute_statement("CREATE INDEX Idf_index ON IdfView (VocId);")
    execute_statement("CREATE INDEX section_term_index ON " + TABLE_SECTION_TERM + " USING BRIN (StemID) WITH (pages_per_range = 16);")
    vacuum()


if __name__ == "__main__":
    pass
