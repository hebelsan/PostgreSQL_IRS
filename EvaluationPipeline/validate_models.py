from ModelA_WPR.Funcs.create_database import get_num_sections as modelA_sections
from ModelA_WPR.Funcs.search import db_search as modelA_results

from ModelB_WVPS.Funcs.create_database import get_num_sections as modelB_sections
from ModelB_WVPS.Funcs.search import db_search as modelB_results

from ModelC_TSV.Funcs.create_database import get_num_sections as modelC_sections
from ModelC_TSV.Funcs.search import db_search as modelC_results

from Solr.solr_fill_data import get_num_sections as solr_sections
from Solr.solr_fill_data import get_num_results as solr_query_results

from Solr.solr_fill_data import solr_ping

from ModelA_WPR.Funcs.ranking import run_test as modela_run_test
from ModelB_WVPS.Funcs.ranking import run_test as modelb_run_test
from ModelC_TSV.Funcs.ranking import run_test as modelc_run_test


def check_ranking_results(query, limit=10):
    results_moda = modela_run_test(query, limit=limit, )
    results_modb = modelb_run_test(query, limit=limit)
    #results_modc = modelc_run_test(query.replace('OR', '|').replace('AND', '&'))
    
    are_same = True
    for i in range(0, len(results_moda)):
        for j in range(0, len(results_moda[i])):
            if results_moda[i][j] != results_modb[i][j]:
                print(results_moda[i])
                print(results_modb[i])
                print(results_moda[i][j])
                print(results_modb[i][j])
                are_same = False
    
    print("ranking results of modela and modelb are same: " + str(are_same))


def is_solr_up():
    result = solr_ping()
    if result == None:
        return False
    elif result["status"] == "OK":
        return True
    return False

def check_num_sections():
    num_sec_moda = modelA_sections()
    num_sec_modb = modelB_sections()
    num_sec_modc = modelC_sections()
    num_sec_modsolr = solr_sections()
    
    same_num_secs = (num_sec_moda == num_sec_modb == num_sec_modc == num_sec_modsolr)
    print("amount of sections are same: " + str(same_num_secs))


def check_num_results_query(query):
    results_secs_a, _ = modelA_results(query)
    results_secs_b = modelB_results(query)
    results_secs_c = modelC_results(query)
    num_results_solr = solr_query_results(query)
    
    '''
    for c in results_secs_c:
        test = False
        for a in results_secs_a:
            if c == a:
                test = True
        if not test:
            print(c)
    '''
    
    print("num query results A: " + str(len(results_secs_a)))
    print("num query results B: " + str(len(results_secs_b)))
    print("num query results C: " + str(len(results_secs_c)))
    print("num query results Solr: " + str(num_results_solr))
    
    is_same = (len(results_secs_a) == len(results_secs_b) == len(results_secs_c) == num_results_solr)
    print("query results are same: " + str(is_same))


if __name__ == "__main__":
    #print(is_solr_up())
    #check_num_sections()
    #check_num_results_query("trump")
    
    check_ranking_results("trump", 100)
    check_ranking_results("trump OR president", 100)
    check_ranking_results("trump AND president", 100)
    check_ranking_results("(trump OR influence) AND (president OR china)", 100)
