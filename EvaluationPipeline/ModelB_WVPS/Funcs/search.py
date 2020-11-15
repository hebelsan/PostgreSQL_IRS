import psycopg2
import psycopg2.extras
from psycopg2.extras import Json
from psycopg2 import sql

from .config import config

import re


##
#   helper function for db connection
##
def get_connection():
    params = config()
    # connect to the PostgreSQL server
    #print("Connecting to the PostgreSQL database...")
    return psycopg2.connect(**params)


def db_search(query, analyse=False, seq_scan="ON"):
    query_terms, flag = parse_query(query)
    ids, sql_string = build_query_string(query_terms, flag, query)
    if sql_string == None:
        return
    if not all(ids):
        print('nothing found...')
        return []
    if analyse:
        sql_string = "EXPLAIN ANALYSE " + sql_string
    
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        if seq_scan == "OFF":
            query = "SET enable_seqscan = OFF;"
            cur.execute(query)
            conn.commit()
        
        cur.execute(sql_string, ids)
        section_ids = cur.fetchall()
        if not section_ids:
            print("term: " + term + " is not in corpora")
        
        section_ids = [sec[0] for sec in section_ids]
        num_results = len(section_ids)
        #if num_results < 20:
            #print(section_ids)
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    
    return section_ids


def build_query_string(query_terms, operator='OR', query = None, query_type='id'):
    query_string = ""
    
    if operator == 'single':
        if query_type == 'id':
            query_string = "SELECT Id FROM Section WHERE Stems @> %s"
        else:
            query_string = "SELECT * FROM Section WHERE Stems @> %s"
    
    if operator == 'OR':
        if query_type == 'id':
            query_string = "SELECT Id FROM Section WHERE Stems && %s"
        else:
            query_string = "SELECT * FROM Section WHERE Stems && %s"
    
    if operator == 'AND':
        if query_type == 'id':
            query_string = "SELECT Id FROM Section WHERE Stems @> %s"
        else:
            query_string = "SELECT * FROM Section WHERE Stems @> %s"
    
    if operator == 'BOTH':
        partials = re.findall('\(.*?\)', query)
        partials_clean = [re.sub('\(|\)', '', partial) for partial in partials]
        res = []
        ids = ()
        for count, part in enumerate(partials_clean):
            terms = []
            flag = '@>'
            if ' OR ' in part:
                terms = part.split(' OR ')
                flag = '&&'
            elif ' AND ' in part:
                terms = part.split(' AND ')
                flag = '@>'
            else:
                terms.append(part)
            res.append([count, flag])
            ids += (tuple([look_up_ids(terms)]))
        for count, part in enumerate(partials):
            query = query.replace(part, str(count))
        ids, query_str = handle_both_operators(query, res, ids, query_type)
        return ids, query_str
    
    return tuple([look_up_ids(query_terms)]), query_string


def look_up_ids(query_terms):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
    
        term_ids = []
        for term in query_terms:
            sql_string = "SELECT Id FROM WordStem WHERE Stem = %s"
            cur.execute(sql_string, (term, ))
            term_id = cur.fetchone()
            if not term_id:
                print("Stem: " + term + " is not in vocabulary")
                continue
            term_ids.append(term_id[0])
        return term_ids
        
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def parse_query(query):
    query_terms = []
    flag = ''
    
    # if contains only OR operator
    if len(query.split(" ")) == 1:
        query_terms = [query]
        flag = 'single'
    
    # if contains only OR operator
    if ' OR ' in query and not ' AND ' in query:
        query_terms = query.split(' OR ')
        flag = 'OR'
    
    # if contains only AND operator
    if ' AND ' in query and not ' OR ' in query:
        query_terms = query.split(' AND ')
        flag = 'AND'
    
    # if contains OR and AND operator
    if ' OR ' in query and ' AND ' in query:
        flag = 'BOTH'
        query_terms = re.split(' AND | OR ', re.sub('\(|\)', '', query))
    
    return query_terms, flag


def handle_both_operators(query, res, ids, query_type='id'):
    query_string = ""
    if query_type == 'id':
        query_string = "SELECT Id FROM Section WHERE"
    else:
        query_string = "SELECT * FROM Section WHERE"
    tokens = query.split(' ')
    for token in tokens:
        if token.isdigit():
            for result in res:
                if result[0] == int(token):
                    query_string += " Stems " + result[1] + " %s"
        else:
            query_string += " " + token
    return ids, query_string


def get_num_results(query):
    result_ids = db_search(query)
    return len(result_ids)

if __name__ == "__main__":
    db_search("(result OR find) AND (paragraph)")
    
