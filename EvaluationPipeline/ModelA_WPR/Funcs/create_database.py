import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql

from math import sqrt
import pandas as pd
import dask.dataframe as dd
import time

from .config import config
from .text_processing import *



TABLE_VOCAB = "Vocabulary"
TABLE_WORD_STEM = "WortStem"
TABLE_DOCUMENT = "Document"
TABLE_SECTION = "Section"
TABLE_SECTION_TERM = "SectionTerm"

TABLES = [TABLE_VOCAB, TABLE_WORD_STEM, TABLE_DOCUMENT,
            TABLE_SECTION, TABLE_SECTION_TERM]

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
                    StemId INTEGER REFERENCES " + TABLE_WORD_STEM + " (Id));")
                    
    cur.execute("CREATE TABLE " + TABLE_DOCUMENT + " ( \
                    Id SERIAL PRIMARY KEY, \
                    Title TEXT, \
                    NumSections INTEGER);")
    
    cur.execute("CREATE TABLE " + TABLE_SECTION + " ( \
                    Id SERIAL PRIMARY KEY, \
                    DocId INTEGER REFERENCES " + TABLE_DOCUMENT + " (Id), \
                    NumWords INTEGER, \
                    Weight REAL);")
    
    cur.execute("CREATE TABLE " + TABLE_SECTION_TERM + " ( \
                    SectionId INTEGER REFERENCES " + TABLE_SECTION + " (Id), \
                    VocabID INTEGER REFERENCES " + TABLE_VOCAB + " (Id), \
                    StemID INTEGER REFERENCES " + TABLE_WORD_STEM + " (Id), \
                    Offsets INTEGER[], \
                    Tf INTEGER);")
    
    cur.execute("CREATE  MATERIALIZED VIEW RankVars AS \
                 SELECT COUNT(*) AS NumSecs, \
                    SUM(NumWords) AS NumWordsSecs, \
                    cast(AVG(NumWords) AS REAL) AS AvgSecLen, \
                    1.7 AS var_K1, 0.75 AS var_b, \
                    1.2 AS ext_K1, 0.9 AS ext_b \
                    FROM Section;")
    
    cur.execute("CREATE MATERIALIZED VIEW IdfView AS \
                 SELECT VocId, DF, \
                    cast(log(cast(NumSecs AS REAL) / DF) AS REAL) AS IDF, \
                    cast(log(1+ ((NumSecs - DF + 0.5) / (DF + 0.5))) AS REAL) AS BM25IDF \
                 FROM (SELECT StemId AS VocId, COUNT(*) AS DF \
                       FROM SectionTerm \
                       GROUP BY StemId) AS GroupedTerms, RankVars;")
    
    # add indices for vocab for faster inserting of data
    cur.execute("CREATE INDEX stem_idx ON " + TABLE_WORD_STEM + "(Stem) include (Id);")
    cur.execute("CREATE INDEX voc_idx ON " + TABLE_VOCAB + "(Term) include (Id, StemId);")
    
    conn.commit()
    print("succefully created tables!")


def add_indices(cur):
    cur.execute("DROP INDEX voc_idx;")

    cur.execute("CREATE INDEX voc_idx ON " + TABLE_VOCAB + "(Term) include (Id);")
    cur.execute("CREATE INDEX doc_idx ON " + TABLE_DOCUMENT + "(Id) include (Title);")
    cur.execute("CREATE INDEX sec_idx ON " + TABLE_SECTION + "(Id) include (NumWords, DocId);")
    cur.execute("CREATE INDEX idf_idx ON IdfView (VocId) include (IDF, BM25IDF);")
    cur.execute("CREATE INDEX sec_term_id ON " + TABLE_SECTION_TERM + "(StemID, SectionId);")
    cur.execute("CREATE INDEX sec_term_all ON " + TABLE_SECTION_TERM + "(StemID, SectionId, tf);")
    ## validate
    '''
    EXPLAIN ANALYSE SELECT Id FROM WortStem WHERE Stem = 'trump';
    EXPLAIN ANALYSE SELECT Term, Id, StemId FROM Vocabulary WHERE Term = 'trump';
    EXPLAIN ANALYSE SELECT Title FROM Document WHERE Id = 103;
    EXPLAIN ANALYSE SELECT DocId FROM Section WHERE Id = 103;
    EXPLAIN ANALYSE SELECT NumWords FROM Section WHERE Id = 103;
    EXPLAIN ANALYSE SELECT SectionId FROM SectionTerm WHERE StemId = 103;
    EXPLAIN ANALYSE SELECT IDF FROM IdfView WHERE VocId = 103;
    EXPLAIN ANALYSE SELECT BM25IDF FROM IdfView WHERE VocId = 103;
    '''


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
    ##
    # DOCUMENT
    ##
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
        term_lexeme_pair = sec[0]
        doc_terms += term_lexeme_pair.keys()
        doc_lexemes += term_lexeme_pair.values()
    
    if len(doc_lexemes) > 0:
        args_str = ",".join("('%s')" % (x) for x in doc_lexemes)
        cur.execute("INSERT INTO WortStem (Stem) VALUES " + args_str + " ON CONFLICT DO NOTHING;")
    
    cur.execute("SELECT Stem, Id FROM WortStem WHERE Stem = ANY(%s);", (doc_lexemes, ))
    lex_id_tuples = cur.fetchall()
    lex_id_dict = {}
    for lex, id in lex_id_tuples:
        lex_id_dict[lex] = id
    vocab = []
    for indx in range(len(doc_terms)):
        vocab.append((doc_terms[indx], lex_id_dict[doc_lexemes[indx]]))
    
    if len(vocab) > 0:
        args_str = ",".join("('%s', '%s')" % (x, y) for (x, y) in vocab)
        cur.execute("INSERT INTO Vocabulary (Term, StemID) VALUES " + args_str + " ON CONFLICT DO NOTHING;")
    
    cur.execute("SELECT Term,Id,StemId FROM Vocabulary WHERE TERM = ANY(%s);", (doc_terms, ))
    term_id_stemid_tuples = cur.fetchall()
    term_id_dict = {}
    for term, id, stemid  in term_id_stemid_tuples:
        term_id_dict[term] = (id, stemid)
    
    
    for sec in sections:
        sec_offsets = sec[1]
        num_words = sec[2]
        
        ##
        # Section
        ##
        if num_words == 0:
            sql_string = "INSERT INTO " + TABLE_SECTION + " \
                                (DocID, NumWords, Weight) \
                            VALUES \
                                (%s, %s, %s) \
                            RETURNING Id;"
            cur.execute(sql_string, (doc_id, num_words, 0))
            continue
        
        sql_string = "INSERT INTO " + TABLE_SECTION + " \
                            (DocID, NumWords, Weight) \
                        VALUES \
                            (%s, %s, %s) \
                        RETURNING Id;"
        cur.execute(sql_string, (doc_id, num_words, 1/sqrt(num_words)))
        sec_id = cur.fetchone()[0]
        
        ##
        # SectionTerm
        ##
        termsec = []
        for term, offsets in sec_offsets.items():
            termsec.append((sec_id, term_id_dict[term][0], term_id_dict[term][1], offsets))
            
        # insert each term in sectionterm
        if len(termsec) > 0:
            args_str = ",".join("('%s', '%s', '%s', '%s', '%s')" % (sec_id, v_id, s_id, "{"+str(offsets)[1:-1]+"}", len(offsets)) for (sec_id, v_id, s_id, offsets) in termsec)
            cur.execute("INSERT INTO SectionTerm (SectionId, VocabID, StemID, Offsets, Tf) VALUES " + args_str + ";")


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
        
        # delete in sectionterm
        deleted_terms = []
        for sec_id in section_ids:
            sql_string = "DELETE FROM " + TABLE_SECTION_TERM + " WHERE SectionId = %s RETURNING (vocabid);"
            cur.execute(sql_string, (sec_id, ))
            term_ids = cur.fetchall()
            if term_ids:
                deleted_terms += term_ids
        
        # delete sections
        sql_string = "DELETE FROM " + TABLE_SECTION + " WHERE docid = %s;"
        cur.execute(sql_string, (doc_id, ))
        
        # delete document
        sql_string = "DELETE FROM " + TABLE_DOCUMENT + " WHERE id = %s;"
        cur.execute(sql_string, (doc_id, ))
        
        # delete from vocab
        if with_vocab:
            stem_ids = []
            deleted_terms = [term_id[0] for term_id in deleted_terms]
            for term_id in deleted_terms:
                # check if it has entries in sectionterm table then do not delete
                sql_string = "SELECT * FROM " + TABLE_SECTION_TERM + " WHERE vocabid = %s"
                cur.execute(sql_string, (term_id, ))
                exists = cur.fetchone()
                if exists:
                    continue
                sql_string = "DELETE FROM " + TABLE_VOCAB + " WHERE id = %s RETURNING (StemID);"
                cur.execute(sql_string, (term_id, ))
                stem_id = cur.fetchone()
                if stem_id:
                    stem_ids.append(stem_id[0])
            for stem_id in stem_ids:
                sql_string = "DELETE FROM " + TABLE_WORD_STEM + " WHERE id = %s;"
                cur.execute(sql_string, (stem_id, ))
        
        
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
        
        print(TABLE_SECTION_TERM + ":")
        cur.execute("SELECT * FROM "+ TABLE_SECTION_TERM + ";")
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
        
        cur.execute("SELECT count(*) FROM "+ TABLE_VOCAB + ";")
        print("Vocabulary: " + str(cur.fetchone()[0]))
        
        cur.execute("SELECT count(*) FROM "+ TABLE_WORD_STEM + ";")
        print("Stems: " + str(cur.fetchone()[0]))
        
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


def db_insert_test_data():
    TEST_DOC = "find and results in the first paragraph\n\n The next paragraph has find in it\n\n find the last has again both at the beginning and end result result find"
    section_data = normalize_document(TEST_DOC)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        db_insert_document("Test Title", section_data, cur)
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
        conn.autocommit = True
        cur.execute("VACUUM SectionTerm;")
        conn.commit()
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
    
    
    
