from ModelA_WPR.Funcs.ranking import db_rank_sec_tfidf as modela_tfidf
from ModelA_WPR.Funcs.ranking import db_rank_sec_bm25 as modela_bm25
from ModelA_WPR.Funcs.ranking import db_rank_sec_tfidf_ext as modela_tfidf_ext
from ModelA_WPR.Funcs.ranking import db_rank_sec_bm25_ext as modela_bm25_ext

from ModelB_WVPS.Funcs.ranking import db_rank_sec_tfidf as modelb_tfidf
from ModelB_WVPS.Funcs.ranking import db_rank_sec_bm25 as modelb_bm25
from ModelB_WVPS.Funcs.ranking import db_rank_sec_tfidf_ext as modelb_tfidf_ext
from ModelB_WVPS.Funcs.ranking import db_rank_sec_bm25_ext as modelb_bm25_ext


def check_ranking_results(query):
    results_moda = modela_run_test(query)
    results_modb = modelb_run_test(query)
    #results_modc = modelc_run_test(query.replace('OR', '|'))
    
    are_same = True
    for i in range(0, len(results_moda)):
        if results_moda[i] != results_modb[i]:
            are_same = False
    
    print("ranking results of modela and modelb are same: " + str(are_same))


def is_solr_up():
    result = solr_ping()
    if result == None:
        return False
    elif result["status"] == "OK":
        return True
    return False

def check_ranking_moda(query):
    scores, secs = modela_tfidf(query)
    print("score tfidf: ")
    print(scores)
    scores, secs = modela_bm25(query)
    print("score bm25: ")
    print(scores)
    scores, secs = modela_tfidf_ext(query)
    print("score tfidf_ext: ")
    print(scores)
    scores, secs = modela_bm25_ext(query)
    print("score bm25_ext: ")
    print(scores)


if __name__ == "__main__":
    check_ranking_moda("second AND twice")
