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


def db_search(query, analyse=False):
    sql_string, query_ids = parse_query(query)
    
    if analyse:
        sql_string = "EXPLAIN ANALYSE " + sql_string
    
    if len(query_ids) == 0:
        return [],[]
        
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        cur.execute(sql_string)
        section_ids = cur.fetchall()
        if not section_ids:
            print("term: " + term + " is not in vocabulary")
        
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
    
    return section_ids, query_ids


def __build_query_string(query_ids, operator='OR', query=None):
    query_string = ""
    
    if operator == 'single' and len(query_ids) > 0:
        query_string = "SELECT DISTINCT SectionId FROM SECTIONTERM WHERE StemId = " + str(query_ids[0])
    
    if operator == 'OR':
        query_string = "SELECT DISTINCT SectionId FROM SECTIONTERM WHERE StemId = ANY(ARRAY" + str(query_ids) + ")"
    
    if operator == 'AND':
        query_string = "SELECT SectionId FROM SectionTerm WHERE StemId = ANY(ARRAY" + str(query_ids) 
        query_string += ") GROUP BY SectionId HAVING COUNT(DISTINCT StemId) = "
        query_string += str(len(query_ids))
    
    if operator == 'BOTH':
        query_string = "SELECT DISTINCT tbl0.SectionId FROM ("
        tokens = query[0].split(" ")
        or_counter = 0
        ands = []
        for token in tokens:
            if token.isdigit():
                part_query = ""
                for entry in query_ids:
                    if entry[0] == int(token):
                        part_query = entry[1]
                query_string += part_query
            if token == "AND":
                ands.append(("tbl" + str(len(ands)), len(ands)))
                query_string += ") tbl"+ str(len(ands)-1) + " INNER JOIN ("
            if token == "OR":
                query_string += ") or" + str(or_counter) + " UNION ("
                or_counter += 1
        
        if len(ands) > 0:
            ands.append(("tbl" + str(len(ands)), len(ands)))
            query_string += ") tbl" + str(len(ands)-1) + " ON tbl0.sectionid = tbl1.sectionid"
            for i in range(1, len(ands)-1):
                query_string += "AND tbl" + str(i) + ".sectionid = tbl" + str(i+1) + ".sectionid"
        else:
            query_string += ")"
            # remove tbl0. and start of query
            query_string = query_string[:16] + query_string[21:]
        query_ids = query[1]
    
    return query_string, query_ids


def __look_up_ids(query_terms):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
    
        term_ids = []
        for term in query_terms:
            sql_string = "SELECT Id FROM WortStem WHERE Stem = %s"
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

        partials_res = [] # [0]:id [1]:query_string [2]:original
        partials = re.findall('\(.*?\)', query)
        partials_clean = [re.sub('\(|\)', '', partial) for partial in partials]
        term_ids = []
        for count, part in enumerate(partials_clean):
            partials_res.append((count, parse_query(part)[0], partials[count]))
            term_ids += parse_query(part)[1]
        
        for part in partials_res:
            query = query.replace(part[2], str(part[0]))
        
        left_over = query.split(" ")
        max_id = len(partials_res)
        for token in left_over:
            if 'OR' not in token and 'AND' not in token and not token.isdigit():
                query = query.replace(token, str(max_id))
                partials_res.append((max_id, parse_query(token)[0]))
                term_ids.append(parse_query(part)[1])
                max_id += 1
        return __build_query_string(partials_res, operator=flag, query=(query, term_ids))
    
    ids = __look_up_ids(query_terms)
    return __build_query_string(ids, operator=flag)


def get_num_results(query):
    result_ids, query_ids = db_search(query)
    return len(result_ids)


if __name__ == "__main__":
    db_search("(paragraph AND Google) OR (Google AND company)")
    
