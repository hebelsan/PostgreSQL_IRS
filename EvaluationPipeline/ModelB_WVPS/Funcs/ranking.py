import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
import math

from .config import config
from .search import *

import re


TABLE_DOCUMENT = "Document"
TABLE_SECTION = "Section"

TABLES = [TABLE_DOCUMENT, TABLE_SECTION]

def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper

##
#   helper function for db connection
##
def get_connection():
    params = config()
    # connect to the PostgreSQL server
    #print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


##
#  ranking
##

def db_rank_sec_tfidf(query, analyse=False, limit=10, withOrderLim=True):
    alltuples, section_sql_string = __get_params(query)
    if alltuples == None:
        return
    
    full_query = "SELECT id, rank_tfidf(Stems, %s, Frequencies, Idfs) AS idf_res \
                  FROM (" + section_sql_string + ") AS FilteredSecs, \
                  (SELECT array_agg(ARRAY[termid, idf]) AS Idfs \
                  FROM IdfView WHERE TermId = ANY(%s)) AS InnerIdf ORDER BY idf_res DESC, id DESC LIMIT " + str(limit) + ";"
    
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-41]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections

def db_rank_sec_bm25(query, analyse=False, limit=10, withOrderLim=True):
    alltuples, section_sql_string = __get_params(query)
    if alltuples == None:
        return
    
    full_query = "SELECT id, rank_bm25(Stems, %s, Frequencies, Idfs, AvgSecLen, NumWords) AS bm25_res \
                  FROM (" + section_sql_string + ") AS FilteredSecs, RankVars, \
                    (SELECT array_agg(ARRAY[termid, Bm25Idf]) AS Idfs \
                    FROM IdfView WHERE TermId = ANY(%s)) AS InnerIdf \
                  ORDER BY bm25_res DESC, id DESC LIMIT " + str(limit) + ";"
    
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-42]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections


def db_rank_sec_tfidf_ext(query, analyse=False, limit=10, withOrderLim=True):
    alltuples, section_sql_string = __get_params(query)
    if alltuples == None:
        return
    
    full_query = "SELECT id, rank_tfidf_ext(Stems, %s, Frequencies, Offsets, Idfs) AS tfidf_ext_res \
                  FROM (" + section_sql_string + ") AS FilteredSecs, \
                  (SELECT array_agg(ARRAY[termid, idf]) AS Idfs \
                  FROM IdfView WHERE TermId = ANY(%s)) AS InnerIdf \
                  ORDER BY tfidf_ext_res DESC, id DESC LIMIT " + str(limit) + ";"
    
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-47]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections


def db_rank_sec_bm25_ext(query, analyse=False, limit=10, withOrderLim=True):
    alltuples, section_sql_string = __get_params(query)
    if alltuples == None:
        return
    
    full_query = "SELECT id, rank_bm25_ext(Stems, %s, Offsets, Frequencies, Idfs, \
                  AvgSecLen, NumWords, NumSecs) AS bm25_ext_res\
                  FROM (" + section_sql_string + ") AS FilteredSecs, RankVars, \
                  (SELECT array_agg(ARRAY[TermId, Bm25Idf]) AS Idfs \
                  FROM IdfView WHERE TermId = ANY(%s)) AS InnerIdf \
                  ORDER BY bm25_ext_res DESC, id DESC LIMIT " + str(limit) + ";"
    
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-46]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections


def __get_params(query):
    query_terms, flag = parse_query(query)
    search_ids = look_up_ids(query_terms)
    sec_ids_tuple, section_sql_string = build_query_string(query_terms, flag, query, 'all')
    
    if len(search_ids) == 0:
        print("none of the query words was found in the vocabulary")
        return None, None
    
    alltuples = tuple([search_ids]) + sec_ids_tuple + tuple([search_ids])
    
    return alltuples, section_sql_string


def __execute_query(full_query, alltuples, analyse=False):
    res_scores = []
    sections = []
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("Set enable_seqscan TO off;")
        
        cur.execute(full_query, alltuples)
        res = cur.fetchall()
        if analyse:
            for row in res:
                res_scores.append(row)
        else:
            for row in res:
                res_scores.append(truncate(row[1], 3))
                sections.append(row[0])
    
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return res_scores, sections


def run_test(query, limit=10):
    scores = []
    sections = []
    scs, secs = db_rank_sec_tfidf(query, limit=limit)
    scores.append(scs)
    sections.append(secs)
    scs, secs = db_rank_sec_bm25(query, limit=limit)
    scores.append(scs)
    sections.append(secs)
    scs, secs = db_rank_sec_tfidf_ext(query, limit=limit)
    scores.append(scs)
    sections.append(secs)
    scs, secs =  db_rank_sec_bm25_ext(query, limit=limit)
    scores.append(scs)
    sections.append(secs)
    return sections


if __name__ == "__main__":
    run_test("result OR find")
