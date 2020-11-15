import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql
import math

from .config import config
from .search import *


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
#  get titles
##
def get_tf_idf_titles(query, limit=10):
    scores, sections = db_rank_sec_tfidf(query, limit=limit)
    title_query = "SELECT title FROM Section\
                    INNER JOIN Document ON Document.Id = Section.DocId \
                    WHERE Section.Id = ANY(%s)";
    titles = __execute_title_query(title_query, sections)
    return titles, sections, scores

def get_tf_idf_titles_with_doc_length(query, limit=10):
    scores, sections = db_rank_sec_tfidf_doc_length(query, limit=limit)
    title_query = "SELECT title FROM Section\
                    INNER JOIN Document ON Document.Id = Section.DocId \
                    WHERE Section.Id = ANY(%s)";
    titles = __execute_title_query(title_query, sections)
    return titles, sections, scores

def get_bm25_titles(query, limit=10):
    scores, sections = db_rank_sec_bm25(query, limit=limit)
    title_query = "SELECT title FROM Section\
                    INNER JOIN Document ON Document.Id = Section.DocId \
                    WHERE Section.Id = ANY(%s)";
    titles = __execute_title_query(title_query, sections)
    return titles, sections, scores

def get_tf_idf_ext_titles(query, limit=10):
    scores, sections = db_rank_sec_tfidf_ext(query, limit=limit)
    title_query = "SELECT title FROM Section\
                    INNER JOIN Document ON Document.Id = Section.DocId \
                    WHERE Section.Id = ANY(%s)";
    titles = __execute_title_query(title_query, sections)
    return titles, sections, scores

def get_bm25_ext_titles(query, limit=10, k3=1000):
    scores, sections = db_rank_sec_bm25_ext(query, limit=limit, k3=k3)
    title_query = "SELECT title FROM Section\
                    INNER JOIN Document ON Document.Id = Section.DocId \
                    WHERE Section.Id = ANY(%s)";
    titles = __execute_title_query(title_query, sections)
    return titles, sections, scores

def db_rank_sec_tfidf_doc_length(query, analyse=False, limit=10, withOrderLim=True):
    section_sql_string, query_ids = parse_query(query)
    full_query = "SELECT SectionId, \
                    cast (SUM( (1 + log(tf)) * idf) * Weight as REAL) AS tfidf \
                    FROM (SELECT SectionTerm.SectionId, SectionTerm.StemId, SUM(tf) AS tf \
                           FROM SectionTerm INNER JOIN (" + section_sql_string + ") AS FilteredSecs \
                             ON SectionTerm.SectionId = FilteredSecs.SectionId \
                             WHERE StemId = ANY(%s) \
                           GROUP BY SectionTerm.SectionId,StemId) AS blob \
                             INNER JOIN IdfView \
                                ON VocId = StemId \
                             INNER JOIN Section \
                                ON SectionId = Section.Id \
                    GROUP BY SectionId, Weight ORDER BY tfidf DESC, SectionId DESC LIMIT " + str(limit) + ";"
    alltuples = (query_ids, )
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections

def db_rank_sec_tfidf_ext_with_secs(query, limit=10, analyse=False, withOrderLim=True):
    _, sections = db_rank_sec_tfidf_doc_length(query, limit=limit)
    section_sql_string, query_ids = parse_query(query)
    full_query = "SELECT SectionId, tfidf + \
                    cast(((min_dist_tfidf(offsets) + min_span_tfidf(offsets)) / nj) as real) \
                  AS tfidfExtended FROM \
                    (SELECT jsonb_agg(offsets) AS offsets, SectionId, SUM(tf) AS nj, \
                    SUM( cast( (1 + log(tf)) * cast(idf as real) as real) ) AS tfidf \
                    FROM ( \
                    SELECT array_agg(elements order by elements) as offsets, SectionTerm.SectionId, StemId, \
                    array_length(array_agg(elements order by elements), 1) AS tf \
                        FROM SectionTerm INNER JOIN (" + section_sql_string + ") AS FilteredSecs \
                          ON SectionTerm.SectionId = FilteredSecs.SectionId, \
                        unnest(offsets) as elements \
                        WHERE StemId = ANY(%s) \
                        GROUP BY SectionTerm.SectionId,StemId) AS blob INNER JOIN IdfView \
                      ON VocId = StemId \
                    GROUP BY SectionId) AS innerTfIdf \
                    WHERE SectionId = ANY(%s) \
                    ORDER BY tfidfExtended DESC, SectionId DESC LIMIT " + str(limit) + ";"
    alltuples = (query_ids, sections)
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections, None




##
#  ranking
##
def db_rank_sec_tfidf(query, analyse=False, limit=10, withOrderLim=True):
    section_sql_string, query_ids = parse_query(query)
    
    full_query = "SELECT SectionId, \
                    cast (SUM( (1 + log(tf)) * idf) as real) AS tfidf \
                    FROM (SELECT SectionTerm.SectionId, SectionTerm.StemId, SUM(tf) AS tf \
                           FROM SectionTerm INNER JOIN (" + section_sql_string + ") AS FilteredSecs \
                             ON SectionTerm.SectionId = FilteredSecs.SectionId \
                             WHERE StemId = ANY(%s) \
                           GROUP BY SectionTerm.SectionId,StemId) AS blob \
                             INNER JOIN IdfView \
                                ON VocId = StemId \
                    GROUP BY SectionId ORDER BY tfidf DESC, SectionId DESC LIMIT " + str(limit) + ";"
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-30]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    alltuples = (query_ids, )
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections


def db_rank_sec_bm25(query, analyse=False, limit=10, withOrderLim=True):
    section_sql_string, query_ids = parse_query(query)
    
    full_query = "SELECT SectionId, cast( SUM( ((var_K1 + 1) * tf) / \
                    (var_K1 * ((1- var_b) + var_b * (Section.NumWords/AvgSecLen)) + tf) * BM25IDF ) as real) \
                    AS BM25Res \
                  FROM (SELECT SectionTerm.SectionId, StemId, SUM(tf) AS tf \
                         FROM SectionTerm \
                            INNER JOIN (" + section_sql_string + ") AS FilteredSecs \
                            ON SectionTerm.SectionId = FilteredSecs.SectionId \
                            WHERE StemId = ANY(%s) \
                         GROUP BY SectionTerm.SectionId,StemId) AS blob\
                    INNER JOIN IdfView ON VocId = StemId \
                    INNER JOIN Section ON Section.Id = SectionId, \
                    RankVars  \
                  GROUP BY SectionId \
                  ORDER BY BM25Res DESC, SectionId DESC LIMIT " + str(limit) + ";"
    alltuples = (query_ids,)
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-47]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections


def db_rank_sec_tfidf_ext(query, analyse=False, limit=10, withOrderLim=True):
    section_sql_string, query_ids = parse_query(query)
    
    full_query = "SELECT SectionId, tfidf + \
                    cast(((min_dist_tfidf(offsets) + min_span_tfidf(offsets)) / nj) as real) \
                  AS tfidfExtended FROM \
                    (SELECT jsonb_agg(offsets) AS offsets, SectionId, SUM(tf) AS nj, \
                    SUM( cast( (1 + log(tf)) * cast(idf as real) as real) ) AS tfidf \
                    FROM ( \
                    SELECT array_agg(elements order by elements) as offsets, SectionTerm.SectionId, StemId, \
                    array_length(array_agg(elements order by elements), 1) AS tf \
                        FROM SectionTerm INNER JOIN (" + section_sql_string + ") AS FilteredSecs \
                          ON SectionTerm.SectionId = FilteredSecs.SectionId, \
                        unnest(offsets) as elements \
                        WHERE StemId = ANY(%s) \
                        GROUP BY SectionTerm.SectionId,StemId) AS blob INNER JOIN IdfView \
                      ON VocId = StemId \
                    GROUP BY SectionId) AS innerTfIdf \
                    ORDER BY tfidfExtended DESC, SectionId DESC LIMIT " + str(limit) + ";"

    
    alltuples = (query_ids, )
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-53]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections


def db_rank_sec_bm25_ext(query, analyse=False, limit=10, withOrderLim=True, k3=1000):
    section_sql_string, query_ids = parse_query(query)
    
    full_query = "SELECT bm25res.SectionId, \
                  cast(min_dist_bm25(offsets, NumSecs, Section.NumWords, \
                    cast(AvgSecLen as real),  BM25IDF, " + str(k3) + ") + cast(BM25 as REAL) as real) AS BM25extended \
        FROM \
        ( \
            SELECT NumSecs, AvgSecLen, SectionId, jsonb_agg(offsets) AS offsets, array_agg(cast(BM25IDF as real)) as bm25idf, \
            SUM(( (ext_K1 + 1) * tf / (ext_K1 * ((1- ext_b) + ext_b * \
            (Section.NumWords/AvgSecLen)) + tf) ) * BM25IDF) AS BM25 \
            FROM \
                (SELECT array_agg(elements order by elements) as offsets, SectionTerm.SectionId, StemId, \
                array_length(array_agg(elements order by elements), 1) AS tf \
                FROM SectionTerm INNER JOIN (" + section_sql_string + ") AS FilteredSecs \
                    ON SectionTerm.SectionId = FilteredSecs.SectionId, \
                unnest(offsets) as elements \
                WHERE StemId = ANY(%s) \
                GROUP BY SectionTerm.SectionId,StemId) AS blob \
            INNER JOIN IdfView ON VocId = StemId \
            INNER JOIN Section ON Section.Id = SectionId, \
            RankVars \
            GROUP BY SectionId, NumSecs, AvgSecLen \
        ) as bm25res \
        INNER JOIN Section \
        ON Section.Id = bm25res.SectionId \
        ORDER BY BM25extended DESC, SectionId DESC LIMIT " + str(limit) + ";"

    alltuples = (query_ids, )
    if analyse and withOrderLim == False:
        full_query = "EXPLAIN ANALYSE " + full_query[:-52]
    elif analyse and withOrderLim == True:
        full_query = "EXPLAIN ANALYSE " + full_query
    
    scores, sections = __execute_query(full_query, alltuples, analyse)
    return scores, sections


def __execute_query(full_query, alltuples, analyse=False):
    scores = []
    sections = []
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute(full_query, alltuples)
        res = cur.fetchall()
        if analyse:
            for row in res:
                scores.append(row)
        else:
            for row in res:
                # truncate because of 
                scores.append(truncate(row[1], 3))
                sections.append(row[0])
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return scores, sections


def __execute_title_query(full_query, sections):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(full_query, (sections, ))
        res = cur.fetchall()
        tiltes = [row[0] for row in res]   
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close() 
    return tiltes


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
