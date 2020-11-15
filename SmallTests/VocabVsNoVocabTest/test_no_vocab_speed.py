import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
from config import config

from nltk.tokenize.regexp import regexp_tokenize
from nltk.corpus import stopwords

import pandas as pd
from string import punctuation
import time
import re

TABLE_FULL_TERM = "FullTerm"
TABLE_VOCAB = "Vocab"
TABLE_VOCAB_BIG = "Vocab_Big"
TABLE_TERMSEC = "TermSec"
TABLE_TERMSEC_BIG = "TermSec_Big"

TABLES = [TABLE_FULL_TERM, TABLE_VOCAB, TABLE_VOCAB_BIG, TABLE_TERMSEC, TABLE_TERMSEC_BIG]

stop = stopwords.words('english')

sec_counter = 0

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
    cur.execute("CREATE TABLE " + TABLE_FULL_TERM + " ( \
                    PassageId INTEGER, \
                    PassageTerm text);")
                    
    cur.execute("CREATE TABLE " + TABLE_VOCAB + " ( \
                    Id SERIAL PRIMARY KEY, \
                    Term TEXT UNIQUE);")
    
    cur.execute("CREATE TABLE " + TABLE_VOCAB_BIG + " ( \
                    Id BIGSERIAL PRIMARY KEY, \
                    Term TEXT UNIQUE);")
    
    cur.execute("CREATE TABLE " + TABLE_TERMSEC + " ( \
                    PassageId INTEGER, \
                    TermId INTEGER);")
    
    cur.execute("CREATE TABLE " + TABLE_TERMSEC_BIG + " ( \
                    PassageId INTEGER, \
                    TermId BIGINT);")
    conn.commit()
    print("succefully created tables!")


def db_create_init_file(database_name, user_name):
    config_file_name = "database.ini"
    if path.exists(config_file_name):
        print("file " + config_file_name + " already exists")
    else:
        f = open(config_file_name,"w+")
        f.write("[postgresql]\n")
        f.write("database=" + database_name + "\n")
        f.write("user=" + user_name)


def add_indices(cur):
    cur.execute("CREATE INDEX voc_indx ON " + TABLE_VOCAB + "(Term);")
    cur.execute("CREATE INDEX voc_big_indx ON " + TABLE_VOCAB_BIG + "(Term);")
    cur.execute("CREATE INDEX ter_id_indx ON " + TABLE_TERMSEC + "(TermId);")
    cur.execute("CREATE INDEX ter_id_big_indx ON " + TABLE_TERMSEC_BIG + "(TermId);")
    cur.execute("CREATE INDEX term_indx ON " + TABLE_FULL_TERM + "(PassageTerm);")


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


def db_insert_data(secs, cur):
    for sec_words in secs:
        global sec_counter
        sec_counter += 1
        
        if len(sec_words) > 0:
            args_str = ",".join("('%s', '%s')" % (sec_counter, term) for term in sec_words)
            cur.execute("INSERT INTO " + TABLE_FULL_TERM + " (PassageId, PassageTerm) VALUES " + args_str + ";")
            
            args_str = ",".join("('%s')" % (term) for term in sec_words)
            cur.execute("INSERT INTO " + TABLE_VOCAB + " (Term) VALUES " + args_str + " ON CONFLICT DO NOTHING;")
            cur.execute("INSERT INTO " + TABLE_VOCAB_BIG + " (Term) VALUES " + args_str + " ON CONFLICT DO NOTHING;")
            
            cur.execute("SELECT Id FROM " + TABLE_VOCAB + " WHERE Term = ANY(%s);", (list(sec_words), ))
            alls_ids = cur.fetchall()
            ids = [id[0] for id in alls_ids]
            
            args_str = ",".join("('%s', '%s')" % (sec_counter, id) for id in ids)
            cur.execute("INSERT INTO " + TABLE_TERMSEC + " (PassageId, TermId) VALUES " + args_str + ";")
            cur.execute("INSERT INTO " + TABLE_TERMSEC_BIG + " (PassageId, TermId) VALUES " + args_str + ";")



def db_show_tables():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        print(TABLE_FULL_TERM + ":")
        cur.execute("SELECT * FROM "+ TABLE_FULL_TERM + ";")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        
        print(TABLE_VOCAB + ":")
        cur.execute("SELECT * FROM "+ TABLE_VOCAB + ";")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("")
        
        print(TABLE_TERMSEC + ":")
        cur.execute("SELECT * FROM "+ TABLE_TERMSEC + ";")
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
        cur.execute("SELECT count(*) FROM "+ TABLE_FULL_TERM + ";")
        print("TotalTerms: " + str(cur.fetchone()[0]))
        
        cur.execute("SELECT count(*) FROM "+ TABLE_VOCAB + ";")
        print("VocabularyEntries: " + str(cur.fetchone()[0]))
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def speed_query(iters, sql_query, args):
    executes = []
    plannings = []
    for i in range(0, iters):
        conn = get_connection()
        cur = conn.cursor()
        execute = 0
        planning = 0
        cur.execute(sql_query, (args, ))
        res = cur.fetchall()
        cur.close()
        if conn is not None:
            conn.close()
        res = [row[0] for row in res]
        for row in res:
            if 'Execution Time' in row:
                execute = re.findall("\d+\.\d+", row)[0]
            if 'Planning Time' in row:
                planning = re.findall("\d+\.\d+", row)[0]
        executes.append(float(execute))
        plannings.append(float(planning))
    return (sum(executes)/iters), (sum(plannings)/iters)

def query_speed(query_terms_array=['trump'], iters=500):
    print("QUERY SPEED:")
    print("queryterm: " + str(query_terms_array))
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # query term frequency
        cur.execute("SELECT count(distinct PassageId) FROM "+ TABLE_FULL_TERM + " WHERE PassageTerm = ANY(%s);", (query_terms_array, ))
        print("Query Terms appears in: " + str(cur.fetchone()[0]) + " sections")
        
        print("***Speed no vocab***")
        min_exe, min_plan = speed_query(iters, "EXPLAIN ANALYSE SELECT PassageId FROM "+ TABLE_FULL_TERM + " WHERE PassageTerm = ANY(%s);", query_terms_array)
        print(query_terms_array[0] + "ExecuteNoVocab: " + str(min_exe) + " " + query_terms_array[0] + "PlanningNoVocab: " + str(min_plan))
        
        print("***Speed vocab INT***")
        min_exe, min_plan = speed_query(iters, "EXPLAIN ANALYSE SELECT Id FROM "+ TABLE_VOCAB + " WHERE Term = ANY(%s);", query_terms_array)
        print(query_terms_array[0] + "ExecuteVocabIntFetchId: " + str(min_exe) + " " + query_terms_array[0] + "PlanningVocabIntFetchId: " + str(min_plan))

        cur.execute("SELECT Id FROM "+ TABLE_VOCAB + " WHERE Term = ANY(%s);", (query_terms_array, ))
        res = cur.fetchall()
        ids = [id[0] for id in res]

        min_exe, min_plan = speed_query(iters, "EXPLAIN ANALYSE SELECT PassageId FROM "+ TABLE_TERMSEC + " WHERE TermId = ANY(%s);", ids)
        print(query_terms_array[0] + "ExecuteVocabIntSearch: " + str(min_exe) + " " + query_terms_array[0] + "PlanningvocabIntSearch: " + str(min_plan))
        
        print("***Speed vocab BigInt***")
        min_exe, min_plan = speed_query(iters, "EXPLAIN ANALYSE SELECT Id FROM "+ TABLE_VOCAB_BIG + " WHERE Term = ANY(%s);", query_terms_array)
        print(query_terms_array[0] + "ExecuteVocabBigIntFetchId: " + str(min_exe) + " " + query_terms_array[0] + "PlanningvocabBigIntFetchId: " +  str(min_plan))

        cur.execute("SELECT Id FROM "+ TABLE_VOCAB_BIG + " WHERE Term = ANY(%s);", (query_terms_array, ))
        res = cur.fetchall()
        ids = [id[0] for id in res]

        min_exe, min_plan = speed_query(iters, "EXPLAIN ANALYSE SELECT PassageId FROM "+ TABLE_TERMSEC_BIG + " WHERE TermId = ANY(%s);", ids)
        print(query_terms_array[0] + "ExecuteVocabBigIntSearch: " + str(min_exe) + " " + query_terms_array[0] + "PlanningvocabBigIntSearch: " + str(min_plan))
        
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
        sql_table_sizes = "SELECT *, pg_size_pretty(total_bytes) AS total \
            , pg_size_pretty(index_bytes) AS index \
            , pg_size_pretty(toast_bytes) AS toast \
            , pg_size_pretty(table_bytes) AS table \
            FROM ( \
            SELECT *, total_bytes-index_bytes-coalesce(toast_bytes,0) AS table_bytes FROM ( \
                SELECT c.oid,nspname AS table_schema, relname AS table_name \
                  , c.reltuples AS row_estimate \
                  , pg_total_relation_size(c.oid) AS total_bytes \
                  , pg_indexes_size(c.oid) AS index_bytes \
                  , pg_total_relation_size(reltoastrelid) AS toast_bytes \
                    FROM pg_class c \
                        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace \
                        WHERE relkind = 'r' \
                ) a \
            ) a \
            WHERE table_schema = 'public' \
            ORDER BY total_bytes DESC;"
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql_table_sizes)
        colnames = [desc[0] for desc in cur.description]
        print(', '.join(colnames))
        rows = cur.fetchall()
        for row in rows:
            print(row)
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
        
        for t_name in TABLES:
            cur.execute("DROP TABLE " + t_name + " CASCADE;")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def normalize_doc(doc):
    paragraphs = doc.split("\n\n")
    secs = []
    for para in paragraphs:
        words = set()
        para = re.sub(",|/|u'\u200b'|‘|—|<|>|@|#|:|\n|\"|\[|\]|\(|\)|-|“|”|’|'|\*", " ", para)
        for token in regexp_tokenize(para.lower(), pattern='\w+|\$[\d\.]+|\S+'):
            if token in punctuation or token == " ":
                continue # ignore punctuation
            elif token in stop:
                pass
            else:
                words.add(token)
    secs.append(words)
    return secs


def insert_passages(percentage):
    df = pd.read_csv('../../../datasets/Arbeitsgruppe/NewsDatasets/news_general_2018_2019_SMALL.csv', header=None)
    df.columns = ['title', 'doc']
    test_dataset = df.sample(frac=percentage)
    start = time.time()
    try:
        conn = get_connection()
        cur = conn.cursor()
        conn.commit()
        test_dataset.apply(lambda x, sec_counter=sec_counter: db_insert_data(normalize_doc(x['doc']), cur), axis=1)
        add_indices(cur)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    print("time: " + str(time.time()-start))


def vacuum_tables():
    conn = None
    try:
        conn = get_connection()
        conn.autocommit = True
        cur = conn.cursor()
        
        for t_name in TABLES:
            cur.execute("VACUUM " + t_name + ";")
        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def pipeline():
    partials = [0.01, 0.05, 0.1, 0.5, 1.0]
    for i in range(0,len(partials)):
        print("******** " + str(partials[i]) + " of the dataset ********")
        db_drop_all_tables()
        db_create_tables()
        insert_passages(percentage=partials[i])
        vacuum_tables()
        db_show_num_entries()
        db_show_memory_usage()
        query_speed(query_terms_array=['much'])
        query_speed(query_terms_array=['trump'])
        query_speed(query_terms_array=['president'])
        query_speed(query_terms_array=['government'])


if __name__ == "__main__":
    pipeline()
    #query_speed(query_terms_array=['government'])
