import pysolr
import json
import pandas as pd

id = 0
            
def insert_test():
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    solr.add([
    {
        "doc_title": "this is a test title",
        "section_text_tf_idf": "This is a section text",
        "section_text_bm25": "This is a section text"
    }])
    solr.commit()


def insert_document(solr, doc, title):
    global id
    sections = doc.split('\n\n')
    for sec in sections:
        if sec != "":
            id += 1
            solr.add([
            {
                "doc_title": title,
                "section_text_tf_idf": sec,
                "section_text_bm25": sec,
                "id": id
            }])


def delete_all():
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    solr.delete(q='*:*')
    solr.commit()


def insert_dataset(df):
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    df.apply(lambda x: insert_document(solr, x['doc'], x['title']), axis=1)
    solr.commit()


def get_num_sections():
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('*:*')
    return result.raw_response['response']['numFound']


def get_num_results(query):
    query = query.replace(' ', '+')
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('section_text_bm25:' + query)
    num_results = result.raw_response['response']['numFound']
    return num_results


def get_rank_titles_tf_idf(query, num_res):
    query = query.replace(' ', '+')
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('section_text_tf_idf:' + query, rows=num_res)
    docs = result.raw_response['response']['docs']
    sections = [int(res['id']) for res in docs ]
    titles = [res['doc_title'] for res in docs ]
    return titles, sections


def get_rank_titles_word_prox_tf_idf(query, num_res):
    query = query.replace('OR', '').replace('AND', '')
    query = query.replace('  ', ' ')
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('section_text_tf_idf:"' + query + '"~10000000', rows=num_res)
    docs = result.raw_response['response']['docs']
    sections = [int(res['id']) for res in docs ]
    titles = [res['doc_title'] for res in docs ]
    return titles, sections


def get_rank_titles_bm25(query, num_res):
    query = query.replace(' ', '+')
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('section_text_bm25:' + query, rows=num_res)
    docs = result.raw_response['response']['docs']
    sections = [int(res['id']) for res in docs ]
    titles = [res['doc_title'] for res in docs ]
    return titles, sections


def get_rank_titles_word_prox_bm25(query, num_res):
    query = query.replace('OR', '').replace('AND', '')
    query = query.replace('  ', ' ')
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('section_text_bm25:"' + query + '"~10000000', rows=num_res)
    docs = result.raw_response['response']['docs']
    sections = [int(res['id']) for res in docs ]
    titles = [res['doc_title'] for res in docs ]
    return titles, sections


def time_solr_bm25(query, num_res):
    query = query.replace(' ', '+')
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('section_text_bm25:' + query, rows=num_res)
    return int(result.raw_response['responseHeader']['QTime'])


def time_solr_bm25_word_prox(query, num_res):
    query = query.replace('OR', '').replace('AND', '')
    query = query.replace('  ', ' ')
    solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
    result = solr.search('section_text_bm25:"' + query + '"~10000000', rows=num_res)
    return int(result.raw_response['responseHeader']['QTime'])


def solr_ping():
    try:
        solr = pysolr.Solr('http://localhost:8983/solr/mycore/', always_commit=False)
        return json.loads(solr.ping())
    except Exception:
        pass
    return None


if __name__ == "__main__":
    insert_test()



# GET SCHEMA:   http://localhost:8983/solr/mycore/schema?wt=schema.xml
# PROXIMITY SEARCH:   section_text:"president trump"~10000000


'''
curl -X POST -H 'Content-type:application/json' --data-binary '{
  "add-field-type" : {
     "name":"text_en_tf_idf",
     "class":"solr.TextField",
     "positionIncrementGap":"100",
     "indexAnalyzer" : {
        "tokenizer":{
           "class":"solr.StandardTokenizerFactory"},
        "filters":[{
           "class":"solr.StopFilterFactory",
           "words":"lang/stopwords_en.txt", 
           "ignoreCase":true },
           {"class":"solr.LowerCaseFilterFactory"},
           {"class":"solr.EnglishPossessiveFilterFactory"},
           {"class":"solr.KeywordMarkerFilterFactory",
            "protected":"protwords.txt"},
           {"class":"solr.PorterStemFilterFactory"}]
        },
      "queryAnalyzer" : {
        "tokenizer":{
           "class":"solr.StandardTokenizerFactory"},
        "filters":[{
           "class":"solr.SynonymGraphFilterFactory",
           "expand":true,
           "ignoreCase":true,
           "synonyms":"synonyms.txt"},
           {"class":"solr.StopFilterFactory",
            "words":"lang/stopwords_en.txt",
            "ignoreCase":true},
           {"class":"solr.LowerCaseFilterFactory"},
           {"class":"solr.EnglishPossessiveFilterFactory"},
           {"class":"solr.KeywordMarkerFilterFactory",
            "protected":"protwords.txt"},
           {"class":"solr.PorterStemFilterFactory"}]
        },
      "similarity": { "class":"org.apache.lucene.search.similarities.ClassicSimilarity"}
    }
}' http://localhost:8983/solr/mycore/schema


curl -X POST -H 'Content-type:application/json' --data-binary '{
  "add-field":{
     "name":"section_text_tf_idf",
     "type":"text_en_tf_idf",
     "indexed":true,
     "stored":true,
     "uninvertible":true}
}' http://localhost:8983/solr/mycore/schema


curl -X POST -H 'Content-type:application/json' --data-binary '{
  "add-field":{
     "name":"section_text_bm25",
     "type":"text_en",
     "indexed":true,
     "stored":true,
     "uninvertible":true}
}' http://localhost:8983/solr/mycore/schema
'''

'''
curl -X POST -H 'Content-type: application/json' -d '{
    "set-property":{"query.filterCache.size":512}
}' http://localhost:8983/solr/mycore/config

curl -X POST -H 'Content-type: application/json' -d '{
    "set-property":{"query.filterCache.initialSize":512}
}' http://localhost:8983/solr/mycore/config


curl -X POST -H 'Content-type: application/json' -d '{
    "set-property":{"query.queryResultCache.size":512}
}' http://localhost:8983/solr/mycore/config

curl -X POST -H 'Content-type: application/json' -d '{
    "set-property":{"query.queryResultCache.initialSize":512}
}' http://localhost:8983/solr/mycore/config


curl -X POST -H 'Content-type: application/json' -d '{
    "set-property":{"query.documentCache.size":512}
}' http://localhost:8983/solr/mycore/config

curl -X POST -H 'Content-type: application/json' -d '{
    "set-property":{"query.documentCache.initialSize":512}
}' http://localhost:8983/solr/mycore/config
'''