import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
import math

from .config import config

import re

def truncate(number, digits) -> float:
    stepper = 10.0 ** digits
    return math.trunc(stepper * number) / stepper


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
#  titles
##
def get_ts_rank_titles(query, limit=10, docfac=0):
    scores, sections = db_rank_ts(query, limit=limit, docfac=docfac)
    title_query = "SELECT title FROM Section\
                    INNER JOIN Document ON Document.Id = Section.DocId \
                    WHERE Section.Id = ANY(%s)";
    titles = __execute_title_query(title_query, sections)
    return titles, sections, scores

def get_ts_rank_cd_titles(query, limit=10, docfac=0):
    scores, sections = db_rank_ts_cd(query, limit=limit, docfac=docfac)
    title_query = "SELECT title FROM Section\
                    INNER JOIN Document ON Document.Id = Section.DocId \
                    WHERE Section.Id = ANY(%s)";
    titles = __execute_title_query(title_query, sections)
    return titles, sections, scores


##
#  ranking
##
def db_rank_ts(query, analyse=False, limit=10, docfac=0):
    query = query.replace("OR", "|").replace("AND", "&")
    sql_string = "SELECT id, ts_rank(Tsvector, query, " + str(docfac) + ") AS rank \
                        FROM " + TABLE_SECTION + ", to_tsquery('english', %s) query \
                        WHERE query @@ Tsvector \
                        ORDER BY rank DESC \
                        LIMIT " + str(limit) + ";"
    scores, sections = __execute_query(sql_string, (query, ), analyse=analyse)
    return scores, sections


def db_rank_ts_cd(query, analyse=False, limit=10, docfac=0):
    query = query.replace("OR", "|").replace("AND", "&")
    sql_string = "SELECT id, ts_rank_cd(Tsvector, query, " + str(docfac) + ") AS rank \
                        FROM " + TABLE_SECTION + ", to_tsquery('english', %s) query \
                        WHERE query @@ Tsvector \
                        ORDER BY rank DESC \
                        LIMIT " + str(limit) + ";"
    scores, sections = __execute_query(sql_string, (query, ), analyse=analyse)
    return scores, sections

'''
def db_rank_sec_tfidf(query, analyse=False, limit=10):
    res_scores = []
    sections = []
    query_terms = re.split(' \| | & ', query)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_string = "SELECT Id, ts_rank_tfidf(Id, %s) AS rank \
                      FROM " + TABLE_SECTION + ", \
                        to_tsquery('english', %s) query \
                      WHERE query @@ Tsvector \
                      ORDER BY rank DESC \
                      LIMIT " + str(limit) + ";"
        if analyse:
            sql_string = "EXPLAIN ANALYSE " + sql_string
        cur.execute(sql_string, (query_terms, query))
        res = cur.fetchall()
        for row in res:
            if analyse:
                res_scores.append(row)
            else:
                res_scores.append(truncate(row[1], 3))
                sections.append(row[0])
    
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return res_scores, sections


def db_rank_sec_bm25(query, analyse=False, limit=10):
    res_scores = []
    sections = []
    query_terms = re.split(' \| | & ', query)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_string = "SELECT Id, ts_rank_bm25(Id, %s) AS rank \
                      FROM " + TABLE_SECTION + ", \
                        to_tsquery('english', %s) query \
                      WHERE query @@ Tsvector \
                      ORDER BY rank DESC \
                      LIMIT " + str(limit) + ";"
        if analyse:
            sql_string = "EXPLAIN ANALYSE " + sql_string
        cur.execute(sql_string, (query_terms, query))
        res = cur.fetchall()
        for row in res:
            if analyse:
                res_scores.append(row)
            else:
                res_scores.append(truncate(row[1], 3))
                sections.append(row[0])
    
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return res_scores, sections


def db_rank_sec_tfidf_ext(query, analyse=False, limit=10):
    res_scores = []
    sections = []
    query_terms = re.split(' \| | & ', query)
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_string = "SELECT Id, ts_rank_tfidf_ext(Id, %s, Tsvector) AS rank \
                      FROM " + TABLE_SECTION + ", \
                        to_tsquery('english', %s) query \
                      WHERE query @@ Tsvector \
                      ORDER BY rank DESC \
                      LIMIT " + str(limit) + ";"
        if analyse:
            sql_string = "EXPLAIN ANALYSE " + sql_string
        cur.execute(sql_string, (query_terms, query))
        res = cur.fetchall()
        for row in res:
            if analyse:
                res_scores.append(row)
            else:
                res_scores.append(truncate(row[1], 3))
                sections.append(row[0])
    
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return res_scores, sections


def db_rank_sec_bm25_ext(query, analyse=False, limit=10):
    query_terms = re.split(' \| | & ', query)
    res_scores = []
    sections = []
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        sql_string = "SELECT Id, ts_rank_bm25_ext(Id, %s, Tsvector) AS rank \
                      FROM " + TABLE_SECTION + ", \
                        to_tsquery('english', %s) query \
                      WHERE query @@ Tsvector \
                      ORDER BY rank DESC \
                      LIMIT " + str(limit) + ";"
        if analyse:
            sql_string = "EXPLAIN ANALYSE " + sql_string
        cur.execute(sql_string, (query_terms, query))
        res = cur.fetchall()
        for row in res:
            if analyse:
                res_scores.append(row)
            else:
                res_scores.append(truncate(row[1], 3))
                sections.append(row[0])
    
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return res_scores, sections
'''

def __execute_query(sql_string, query_tuples, analyse=False):
    scores = []
    section = []
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        if analyse:
            sql_string = "EXPLAIN ANALYSE " + sql_string
        cur.execute(sql_string, query_tuples)
        res = cur.fetchall()
        for row in res:
            if analyse:
                scores.append(row)
            else:
                section.append(row[0])
                scores.append(row[1])
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return scores, section


def __execute_title_query(full_query, sections):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(full_query, (sections, ))
        res = cur.fetchall()
        titles = [row[0] for row in res]   
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close() 
    return titles


def run_test(query):
    pass
    '''
    scores = []
    sections = []
    scs, secs = db_rank_sec_tfidf(query)
    scores.append(scs)
    sections.append(secs)
    scs, secs = db_rank_sec_bm25(query)
    scores.append(scs)
    sections.append(secs)
    scs, secs = db_rank_sec_tfidf_ext(query)
    scores.append(scs)
    sections.append(secs)
    scs, secs = db_rank_sec_bm25_ext(query)
    scores.append(scs)
    sections.append(secs)
    return sections
    '''


if __name__ == "__main__":
    run_test("result | find")
