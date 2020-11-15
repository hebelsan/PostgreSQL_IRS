import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
import json
import time
import pandas as pd
from math import sqrt

from .config import config
from .text_processing import *



TABLE_VOCAB = "Vocabulary"
TABLE_WORD_STEM = "WordStem"
TABLE_DOCUMENT = "Document"
TABLE_SECTION = "Section"

TABLES = [TABLE_VOCAB, TABLE_WORD_STEM, TABLE_DOCUMENT,
            TABLE_SECTION]

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
    cur.execute("CREATE TABLE " + TABLE_WORD_STEM + " ( \
                    Id SERIAL PRIMARY KEY, \
                    Stem TEXT UNIQUE);")
    
    cur.execute("CREATE TABLE " + TABLE_VOCAB + " ( \
                    Id SERIAL PRIMARY KEY, \
                    Term TEXT UNIQUE, \
                    StemID INTEGER REFERENCES " + TABLE_WORD_STEM + " (Id));")
                    
    cur.execute("CREATE TABLE " + TABLE_DOCUMENT + " ( \
                    Id SERIAL PRIMARY KEY, \
                    Title TEXT, \
                    NumSections INTEGER);")
    
    cur.execute("CREATE TABLE " + TABLE_SECTION + " ( \
                    Id SERIAL PRIMARY KEY, \
                    DocID INTEGER REFERENCES " + TABLE_DOCUMENT + " (Id), \
                    Terms INTEGER[], \
                    Stems INTEGER[], \
                    Offsets jsonb, \
                    Frequencies INTEGER[], \
                    NumWords INTEGER, \
                    Weight REAL);")
    
    cur.execute("CREATE  MATERIALIZED VIEW RankVars AS SELECT \
                    COUNT(id) as NumSecs, \
                    SUM(NumWords) AS SumNumWordsSecs, \
                    cast(AVG(NumWords) AS REAL) AS AvgSecLen, \
                    1.7 AS var_K1, 0.75 AS var_b \
                 FROM Section;")
    
    
    cur.execute("CREATE MATERIALIZED VIEW idfview AS \
                    SELECT Termid, \
                           Df, \
                           cast(log(cast(NumSecs AS REAL) / Df) AS REAL) AS Idf, \
                           cast(log(1+ ((NumSecs - Df + 0.5) / (Df + 0.5))) AS REAL) AS Bm25Idf \
                    FROM \
                        (SELECT COUNT(id) AS Df, \
                                UNNEST(Stems) AS TermId \
                         FROM Section GROUP BY TermId) as InnerDf, RankVars;")
    
    conn.commit()
    
    # add indices for vocab for faster inserting of data
    cur.execute("CREATE INDEX stem_index ON " + TABLE_WORD_STEM + "(Stem) include (Id);")
    cur.execute("CREATE INDEX vocab_index ON " + TABLE_VOCAB + "(Term) include (Id, StemId);")
    
    conn.commit()
    print("succefully created tables!")


def add_indices(cur):
    # drop olds
    cur.execute("DROP INDEX stem_index;")
    cur.execute("DROP INDEX vocab_index;")
    # add new
    cur.execute("CREATE INDEX stem_indx ON " + TABLE_WORD_STEM + "(Stem) INCLUDE (Id);")
    cur.execute("CREATE INDEX voc_indx ON " + TABLE_VOCAB + "(Term) INCLUDE (Id);")
    cur.execute("CREATE INDEX doc_indx ON " + TABLE_DOCUMENT + "(Id) INCLUDE (Title);")
    cur.execute("CREATE INDEX sec_indx_w ON " + TABLE_SECTION + "(Id) INCLUDE (NumWords);")
    cur.execute("CREATE INDEX sec_indx_d ON " + TABLE_SECTION + "(Id) INCLUDE (DocId);")
    cur.execute("CREATE INDEX idf_indx ON IdfView (Termid) INCLUDE (IDF, BM25IDF);")
    cur.execute("CREATE INDEX search_index ON " + TABLE_SECTION + " USING GIN (Stems gin__int_ops);")


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


def db_insert_document(doc_title, sections, cur):
    # DOCUMENT
    sql_string = "INSERT INTO " + TABLE_DOCUMENT + " \
                        (Title, NumSections) \
                    VALUES \
                        (%s, %s) \
                    RETURNING Id;"
    cur.execute(sql_string, (doc_title, len(sections)))
    doc_id = cur.fetchone()[0]
    
    ##
    # insert vocab and lexemes
    ##
    doc_terms = []
    doc_lexemes = []
    for sec in sections:
        term_lexeme_pair = lexemes = sec[0]
        doc_terms += term_lexeme_pair.keys()
        doc_lexemes += term_lexeme_pair.values()
    
    # insert lexemes
    if len(doc_lexemes) > 0:
        args_str = ",".join("('%s')" % (x) for x in doc_lexemes)
        cur.execute("INSERT INTO " + TABLE_WORD_STEM + " (Stem) VALUES " + args_str + " ON CONFLICT DO NOTHING;")
    
    cur.execute("SELECT Stem, Id FROM " + TABLE_WORD_STEM + " WHERE Stem = ANY(%s);", (doc_lexemes, ))
    lex_id_tuples = cur.fetchall()
    lex_id_dict = {}
    for i, j in lex_id_tuples:
        lex_id_dict[i] = j
    vocab = []
    for indx in range(len(doc_terms)):
        vocab.append((doc_terms[indx], lex_id_dict[doc_lexemes[indx]]))
    
    if len(vocab) > 0:
        args_str = ",".join("('%s', '%s')" % (x, y) for (x, y) in vocab)
        cur.execute("INSERT INTO " + TABLE_VOCAB + " (Term, StemId) VALUES " + args_str + " ON CONFLICT DO NOTHING;")
    
    cur.execute("SELECT Term,Id,StemId FROM " + TABLE_VOCAB + " WHERE TERM = ANY(%s);", (doc_terms, ))
    term_id_stemid_tuples = cur.fetchall()
    term_id_dict = {}
    for term, id, stemid  in term_id_stemid_tuples:
        term_id_dict[term] = (id, stemid)
    
    
    for sec in sections:
        lexemes = sec[0]
        offsets = sec[1]
        num_words = sec[2]
        
        if num_words == 0:
            sql_string = "INSERT INTO " + TABLE_SECTION + " \
                                (DocID, Terms, Stems, Offsets, Frequencies, NumWords, Weight) \
                            VALUES \
                                (%s, %s, %s, %s, %s, %s, %s);"
            cur.execute(sql_string, (doc_id, [], [], json.dumps([]), [], 0, 0))
            continue
        
        sec_terms = []
        sec_stems = []
        sec_offsets = []
        frequencies = []
        
        # Lexems and Vocab
        for term in lexemes.keys():
            sec_terms.append(term_id_dict[term][0])
            sec_stems.append(term_id_dict[term][1])
            sec_offsets.append(offsets[term])
            frequencies.append(len(offsets[term]))
            
        
        # SECTION
        sql_string = "INSERT INTO " + TABLE_SECTION + " \
                            (DocID, Terms, Stems, Offsets, Frequencies, NumWords, Weight) \
                        VALUES \
                            (%s, %s, %s, %s, %s, %s, %s);"
        cur.execute(sql_string, (doc_id, sec_terms, sec_stems, json.dumps(sec_offsets),
            frequencies, num_words, 1/sqrt(num_words)))


# % param: with_vocab: bool identifieny if doc entries should also be remove from Vocab
def db_delete_document(doc_title, with_vocab=False):
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
        print(TABLE_VOCAB + ":")
        cur.execute("SELECT * FROM "+ TABLE_VOCAB + ";")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        
        print(TABLE_WORD_STEM + ":")
        cur.execute("SELECT * FROM "+ TABLE_WORD_STEM + ";")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        
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
        
        print("IdfView:")
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
        
        cur.execute("SELECT count(*) FROM "+ TABLE_SECTION + " LIMIT 10;")
        print("Sections: " + str(cur.fetchone()[0]))
        
        cur.execute("SELECT count(*) FROM "+ TABLE_VOCAB + " LIMIT 10;")
        print("Vocabulary: " + str(cur.fetchone()[0]))
        
        cur.execute("SELECT count(*) FROM "+ TABLE_WORD_STEM + " LIMIT 10;")
        print("Stems: " + str(cur.fetchone()[0]))
        
        cur.execute("SELECT count(*) FROM idfview LIMIT 10;")
        print("Idf: " + str(cur.fetchone()[0]))
        
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
        
        cur.execute("DROP MATERIALIZED VIEW IdfView;")
        cur.execute("DROP MATERIALIZED VIEW RankVars;")
        
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
        section_data = normalize_document_fast(TEST_DOC)
        db_insert_document("Test Title", section_data, cur)
        
        # Refresh Materialized Views - order is important
        cur.execute("REFRESH MATERIALIZED VIEW RankVars;")
        cur.execute("REFRESH MATERIALIZED VIEW IdfView;")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def db_insert_test_data():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        TEST_DOC = "find and result in the first paragraph\n\n The next paragraph has find in it\n\n find the last has again both at the beginning and end result result find"
        section_data = normalize_document_fast(TEST_DOC)
        db_insert_document("Test Title", section_data, cur)
        
        # Refresh Materialized Views - order is important
        cur.execute("REFRESH MATERIALIZED VIEW RankVars;")
        cur.execute("REFRESH MATERIALIZED VIEW IdfView;")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def db_insert_dataset(df, pipeline=0):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        df.apply(lambda x: db_insert_document(x['title'], normalize_document(x['doc'], pipeline), cur), axis=1)
        
        cur.execute("REFRESH MATERIALIZED VIEW RankVars;")
        cur.execute("REFRESH MATERIALIZED VIEW IdfView;")
        
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
    
    df = pd.read_csv('../../datasets/Arbeitsgruppe/NewsDatasets/news_general_2018_2019_SMALL.csv', header=None)
    df.columns = ['title', 'doc']
    df = df.sample(n=10000)
    
    time1 = time.time()
    db_insert_dataset(df)
    #db_insert_test_data()
    time2 = time.time()
    print("time: " + str(time2 - time1))
    
    
