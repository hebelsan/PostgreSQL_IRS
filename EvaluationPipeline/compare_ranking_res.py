from ModelA_WPR.Funcs.ranking import get_tf_idf_titles as moda_tf_idf
from ModelA_WPR.Funcs.ranking import get_tf_idf_titles_with_doc_length as moda_tf_idf_with_doc
from ModelA_WPR.Funcs.ranking import db_rank_sec_tfidf_ext_with_secs as moda_rerank_tf_idf_with_doc
from ModelA_WPR.Funcs.ranking import get_bm25_titles as moda_bm25
from ModelA_WPR.Funcs.ranking import get_tf_idf_ext_titles as moda_tf_idf_ext
from ModelA_WPR.Funcs.ranking import get_bm25_ext_titles as moda_bm25_ext

from ModelC_TSV.Funcs.ranking import get_ts_rank_titles as modc_ts_rank
from ModelC_TSV.Funcs.ranking import get_ts_rank_cd_titles as modc_ts_rank_cd

from Solr.solr_fill_data import get_rank_titles_tf_idf as solr_tfidf
from Solr.solr_fill_data import get_rank_titles_word_prox_tf_idf as solr_tfidf_proximity
from Solr.solr_fill_data import get_rank_titles_bm25 as solr_bm25
from Solr.solr_fill_data import get_rank_titles_word_prox_bm25 as solr_bm25_proximity


def calc_avg_precision(true_arr, test_arr):
    pos = 0
    neg = 0
    res = []
    for indx in range(0, len(true_arr)):
        if test_arr[indx] in true_arr:
            pos += 1
            res.append(pos/(pos+neg))
        else:
            neg += 1
    if len(res) > 0:
        return sum(res) / len(res)
    return 0

def mean_avg_prec(queries_true, queries_sec, num_res, func_true_sections, func_sections, doclen=None):
    res = []
    for i in range(0, len(queries_true)):
        _, true_sections = func_true_sections(queries_true[i].replace('(', '').replace(')', ''), num_res)
        _, sections, _ = func_sections(queries_sec[i], num_res)
        if doclen != None:
            _, sections, _ = func_sections(queries_sec[i], num_res, doclen)
        avg_prec = calc_avg_precision(true_sections, sections)
        res.append(avg_prec)
    return sum(res)/len(res)


##
#
# TF IDF
#
##
def compare_mean_avg_tf_idf(num_res, queries):
    # adjust queries for other modells then own
    queries_own = queries
    queries_others = [query.replace('(', '').replace(')', '') for query in queries]

    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf, moda_tf_idf)
    print("Own Models TF-IDF: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf, moda_tf_idf_with_doc)
    print("Own Models TF-IDF with doclen: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf, moda_rerank_tf_idf_with_doc)
    print("Own Models TF-IDF with doclen reranked: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf, moda_tf_idf_ext)
    print("Own Models TF-IDF-Extension: " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf, modc_ts_rank)
    print("Tsvector ts_rank (no doclen consideration): " + str(res))

    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf, modc_ts_rank, 1)
    print("Tsvector ts_rank (divides the rank by 1 + the log of the doclen): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf, modc_ts_rank_cd)
    print("Tsvector ts_rank_cd (no doclen consideration): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf, modc_ts_rank_cd, 1)
    print("Tsvector ts_rank_cd (divides the rank by 1 + the log of the doclen): " + str(res))


def compare_mean_avg_tf_idf_ext(num_res, queries):
    # adjust queries for other modells then own
    queries_own = queries
    queries_others = [query.replace('(', '').replace(')', '') for query in queries]

    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf_proximity, moda_tf_idf)
    print("Own Models TF-IDF: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf_proximity, moda_tf_idf_with_doc)
    print("Own Models TF-IDF with doclen: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf_proximity, moda_rerank_tf_idf_with_doc)
    print("Own Models TF-IDF with doclen reranked: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_tfidf_proximity, moda_tf_idf_ext)
    print("Own Models TF-IDF-Extension: " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf_proximity, modc_ts_rank)
    print("Tsvector ts_rank (no doclen consideration): " + str(res))

    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf_proximity, modc_ts_rank, 1)
    print("Tsvector ts_rank (divides the rank by 1 + the log of the doclen): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf_proximity, modc_ts_rank_cd)
    print("Tsvector ts_rank_cd (no doclen consideration): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_tfidf_proximity, modc_ts_rank_cd, 1)
    print("Tsvector ts_rank_cd (divides the rank by 1 + the log of the doclen): " + str(res))


##
#
# BM25
#
##
def compare_mean_avg_bm25(num_res, queries):
    # adjust queries for other modells then own
    queries_own = queries
    queries_others = [query.replace('(', '').replace(')', '') for query in queries]

    res = mean_avg_prec(queries_others, queries_own, num_res, solr_bm25, moda_bm25)
    print("Own Models BM25: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_bm25, moda_bm25_ext)
    print("Own Models BM25-Extension: " + str(res))

    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25, modc_ts_rank)
    print("Tsvector ts_rank (no doclen consideration): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25, modc_ts_rank, 1)
    print("Tsvector ts_rank (divides the rank by 1 + the log of doclen): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25, modc_ts_rank_cd)
    print("Tsvector ts_rank_cd (no doclen consideration): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25, modc_ts_rank_cd, 1)
    print("Tsvector ts_rank_cd (divides the rank by 1 + the log of the doclen): " + str(res))


def compare_mean_avg_bm25_ext(num_res, queries):
    # adjust queries for other modells then own
    queries_own = queries
    queries_others = [query.replace('(', '').replace(')', '') for query in queries]

    res = mean_avg_prec(queries_others, queries_own, num_res, solr_bm25_proximity, moda_bm25)
    print("Own Models BM25: " + str(res))
    res = mean_avg_prec(queries_others, queries_own, num_res, solr_bm25_proximity, moda_bm25_ext)
    print("Own Models BM25-Extension: " + str(res))

    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25_proximity, modc_ts_rank)
    print("Tsvector ts_rank (no doclen consideration): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25_proximity, modc_ts_rank, 1)
    print("Tsvector ts_rank (divides the rank by 1 + the log of doclen): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25_proximity, modc_ts_rank_cd)
    print("Tsvector ts_rank_cd (no doclen consideration): " + str(res))
    res = mean_avg_prec(queries_others, queries_others, num_res, solr_bm25_proximity, modc_ts_rank_cd, 1)
    print("Tsvector ts_rank_cd (divides the rank by 1 + the log of the doclen): " + str(res))


if __name__ == "__main__":
    num_res = 30
    queries = ['trump', 'trump AND president', 'trump OR president', 
                '(trump OR obama) AND (president)', 'reform OR vote OR climate OR discussion']

    print("********TEST TF********")
    compare_mean_avg_tf_idf(num_res, queries)
    print()
    print("********TEST TF-IDF Word Poximity********")
    compare_mean_avg_tf_idf_ext(num_res, queries)
    print()
    print("********TEST BM25********")
    compare_mean_avg_bm25(num_res, queries)
    print()
    print("********TEST BM25 Word Proximity********")
    compare_mean_avg_bm25_ext(num_res, queries)
