import psycopg2
import requests

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


sql_index_size_seperate = "SELECT table_name, index_bytes AS index \
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
    WHERE table_schema = 'public';"


sql_index_size_sum = "SELECT SUM(index_bytes) AS index \
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
    WHERE table_schema = 'public';"


def size_databases():
    print("\n*******Model Sizes*******")
    ## Size of modela modelb modelc
    try:
        conn = psycopg2.connect("dbname='modela' user='alex'")
        cur = conn.cursor()
        sql_query = "SELECT d.datname as Name,  pg_catalog.pg_get_userbyid(d.datdba) as Owner, \
                CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT') \
                  THEN \
                    pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) \
                  ELSE 'No Access' \
                END as Size \
            FROM pg_catalog.pg_database d where pg_catalog.pg_get_userbyid(d.datdba) = 'alex' \
                order by \
                CASE WHEN pg_catalog.has_database_privilege(d.datname, 'CONNECT') \
                  THEN pg_catalog.pg_database_size(d.datname) \
                  ELSE NULL \
                END desc \
                LIMIT 20";
        cur.execute(sql_query)
        rows = cur.fetchall()
        for row in rows:
            print("Model: " + row[0] + " Size: " + row[2])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    ## Size of Solr
    response = requests.get('http://localhost:8983/solr/mycore/replication?command=details').json()
    solr_size = response["details"]["indexSize"]
    print("Model: Solr" + " Size: " + str(solr_size))


def table_sizes_modela():
    print("\n*******Model A Table Sizes*******")
    try:
        conn = psycopg2.connect("dbname='modela' user='alex'")
        cur = conn.cursor()
        cur.execute(sql_table_sizes)
        colnames = [desc[0] for desc in cur.description]
        print(', '.join(colnames))
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def index_size_modela(sum=True):
    try:
        conn = psycopg2.connect("dbname='modela' user='alex'")
        cur = conn.cursor()
        res = None
        if sum:
            cur.execute(sql_index_size_sum)
            res = cur.fetchone()[0]
        else:
            res = []
            cur.execute(sql_index_size_seperate)
            values = cur.fetchall()
            for row in values:
                res.append((row[0], row[1]))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return res


def index_size_modelb(sum=True):
    try:
        conn = psycopg2.connect("dbname='modelb' user='alex'")
        cur = conn.cursor()
        res = None
        if sum:
            cur.execute(sql_index_size_sum)
            res = cur.fetchone()[0]
        else:
            res = []
            cur.execute(sql_index_size_seperate)
            values = cur.fetchall()
            for row in values:
                res.append((row[0], row[1]))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    return res


def table_sizes_modelb():
    print("\n*******Model B Table Sizes*******")
    try:
        conn = psycopg2.connect("dbname='modelb' user='alex'")
        cur = conn.cursor()
        cur.execute(sql_table_sizes)
        colnames = [desc[0] for desc in cur.description]
        print(', '.join(colnames))
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def table_sizes_modelc():
    print("\n*******Model Tsvector Table Sizes*******")
    try:
        conn = psycopg2.connect("dbname='tsvector' user='alex'")
        cur = conn.cursor()
        cur.execute(sql_table_sizes)
        colnames = [desc[0] for desc in cur.description]
        print(', '.join(colnames))
        rows = cur.fetchall()
        for row in rows:
            print(row)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    size_databases()
    table_sizes_modela()
    table_sizes_modelb()
    table_sizes_modelc()
