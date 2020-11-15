import re
from functools import partial


from ModelA_WPR.Funcs.search import db_search as modelA_search
from ModelA_WPR.Funcs.ranking import db_rank_sec_tfidf as modela_tfidf
from ModelA_WPR.Funcs.ranking import db_rank_sec_bm25 as modela_bm25
from ModelA_WPR.Funcs.ranking import db_rank_sec_tfidf_ext as modela_tfidf_ext
from ModelA_WPR.Funcs.ranking import db_rank_sec_bm25_ext as modela_bm25_ext

from ModelB_WVPS.Funcs.search import db_search as modelB_search
from ModelB_WVPS.Funcs.ranking import db_rank_sec_tfidf as modelb_tfidf
from ModelB_WVPS.Funcs.ranking import db_rank_sec_bm25 as modelb_bm25
from ModelB_WVPS.Funcs.ranking import db_rank_sec_tfidf_ext as modelb_tfidf_ext
from ModelB_WVPS.Funcs.ranking import db_rank_sec_bm25_ext as modelb_bm25_ext

from ModelC_TSV.Funcs.search import db_search as modelC_search
from ModelC_TSV.Funcs.ranking import db_rank_ts as ts_rank
from ModelC_TSV.Funcs.ranking import db_rank_ts_cd as ts_rank_cd

from Solr.solr_fill_data import time_solr_bm25
from Solr.solr_fill_data import time_solr_bm25_word_prox

from ModelA_WPR.Funcs.search import db_search as modelA_results



def print_explanation(has_two_arg, func):
    res = None
    if has_two_arg:
        res, _ = func()
    else:
        res = func()
    for row in res:
        if type(row) is tuple:
            row = row[0]
        print(row)


def perform_iterations(has_two_arg, iterations, func):
    execute_plan_tuples = []
    for i in range(0, iterations):
        res = None
        execute = 0
        planning = 0
        if has_two_arg:
            res, _ = func()
        else:
            res = func()
        for row in res:
            if type(row) is tuple:
                row = row[0]
            if 'Execution Time' in row:
                execute = float(re.findall("\d+\.\d+", row)[0])
            elif 'Planning Time' in row:
                planning = float(re.findall("\d+\.\d+", row)[0])
        execute_plan_tuples.append((execute, planning))
    return min(execute_plan_tuples)[0], min(execute_plan_tuples)[1]


##
#   Explain Querys
##
def explain_query_modela(query, limit=10, withOrderLim=True):
    print()
    print("***Results Model A***")
    print("******Query: " + query + "******")
    print("****** SEARCH ******")
    print_explanation(True, partial(modelA_search ,query, True))
    print("****** Ranking ******")
    print_explanation(True, partial(modela_tfidf, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print_explanation(True, partial(modela_bm25, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print_explanation(True, partial(modela_tfidf_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print_explanation(True, partial(modela_bm25_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))


def explain_query_modelb(query, limit=10, withOrderLim=True):
    print()
    print("***Results Model B***")
    print("******Query: " + query + "******")
    print("****** SEARCH ******")
    print_explanation(False, partial(modelB_search ,query, True, "OFF"))
    print("****** Ranking ******")
    print_explanation(True, partial(modelb_tfidf, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print_explanation(True, partial(modelb_bm25, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print_explanation(True, partial(modelb_tfidf_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print_explanation(True, partial(modelb_bm25_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))


##
#   Analyse Query time
##
def analyse_query_time_modela(query, iterations=50, limit=10, withOrderLim=True, withExtension=True):
    print()
    print("***Results Model A***")
    print("******Query: " + query + "******")
    print("******SEARCH SPEED******")
    execution_time, planning_time = perform_iterations(True, iterations, partial(modelA_search ,query, True))
    print("ExecutionTimeA_search: " + str(execution_time) + " PlanningTimeA_search: " + str(planning_time))
    print("******Ranking SPEED******")
    execution_time, planning_time = perform_iterations(True, iterations,
        partial(modela_tfidf, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print("ExecutionTimeA_tfidf: " + str(execution_time) + " PlanningTimeA_tfidf: " + str(planning_time))
    execution_time, planning_time = perform_iterations(True, iterations,
        partial(modela_bm25, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print("ExecutionTimeA_bm25: " + str(execution_time) + " PlanningTimeA_bm25: " + str(planning_time))
    if withExtension:
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modela_tfidf_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeA_tfidfext: " + str(execution_time) + " PlanningTimeA_tfidfext: " + str(planning_time))
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modela_bm25_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeA_bm25ext: " + str(execution_time) + " PlanningTimeA_bm25ext: " + str(planning_time))


def analyse_query_time_modelb(query, iterations=50, limit=10, withOrderLim=True, withExtension=True):
    print()
    print("***Results Model B***")
    print("******Query: " + query + "******")
    print("******SEARCH SPEED******")
    execution_time, planning_time = perform_iterations(False, iterations, partial(modelB_search, query, True, "OFF"))
    print("ExecutionTimeB_search: " + str(execution_time) + " PlanningTimeB_search: " + str(planning_time))
    print("******Ranking SPEED******")
    execution_time, planning_time = perform_iterations(True, iterations,
        partial(modelb_tfidf, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print("ExecutionTimeB_tfidf: " + str(execution_time) + " PlanningTimeB_tfidf: " + str(planning_time))
    execution_time, planning_time = perform_iterations(True, iterations,
        partial(modelb_bm25, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
    print("ExecutionTimeB_bm25: " + str(execution_time) + " PlanningTimeB_bm25: " + str(planning_time))
    if withExtension:
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modelb_tfidf_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeB_tfidfext: " + str(execution_time) + " PlanningTimeB_tfidfext: " + str(planning_time))
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modelb_bm25_ext, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeB_bm25ext: " + str(execution_time) + " PlanningTimeB_bm25ext: " + str(planning_time))

def analyse_query_time_modelc(query, iterations=50):
    print()
    print("***Results Model c***")
    print("******Query: " + query + "******")
    print("******SEARCH SPEED******")
    execution_time, planning_time = perform_iterations(False, iterations,
        partial(modelC_search ,query.replace("OR", "|").replace("AND", "&"), True))
    print("ExecutionTimeC_search: " + str(execution_time) + " PlanningTimeC_search: " + str(planning_time))
    print("******Ranking SPEED******")
    execution_time, planning_time = perform_iterations(True, iterations, partial(ts_rank,
                                        query.replace("OR", "|").replace("AND", "&"), True))
    print("ExecutionTimeC_tsrank: " + str(execution_time) + " PlanningTimeC_tsrank: " + str(planning_time))




def test_query_factors(queries, iterations=50, limit=10, withOrderLim=True):
    for query in queries:
        print("**QUERY: " + query)
        #results_secs_a, _ = modelA_results(query)
        #print(len(results_secs_a))
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modela_bm25, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeA_bm25: " + str(execution_time) + " PlanningTimeA_bm25: " + str(planning_time))
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modelb_bm25, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeB_bm25: " + str(execution_time) + " PlanningTimeB_bm25: " + str(planning_time))
        '''
        # Mod A
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modela_tfidf, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeA_tfidf: " + str(execution_time) + " PlanningTimeA_tfidf: " + str(planning_time))
        # Mod B
        execution_time, planning_time = perform_iterations(True, iterations,
            partial(modelb_tfidf, query=query, analyse=True, limit=limit, withOrderLim=withOrderLim))
        print("ExecutionTimeB_tfidf: " + str(execution_time) + " PlanningTimeB_tfidf: " + str(planning_time))
        # Mod C
        execution_time, planning_time = perform_iterations(True, iterations, partial(ts_rank, query, True, limit=limit, docfac=0))
        print("ExeTimeC_ts_rank: " + str(execution_time) + " PlanningTimeC_tsrank: " + str(planning_time))
        execution_time, planning_time = perform_iterations(True, iterations, partial(ts_rank_cd, query, True, limit=limit, docfac=0))
        print("ExeTimeC_ts_rank_cd: " + str(execution_time) + " PlanningTimeC_tsrank: " + str(planning_time))
        '''
        print()


def test_query_solr(query, limit=10):
    print("ExeTimeSolrBM25: " + str(time_solr_bm25(query, limit)))
    #print("ExeTimeSolrBM25WordProx: " + str(time_solr_bm25_word_prox(query, limit)))



if __name__ == "__main__":
    queries = ['home', 'home AND good', 'home AND good AND report', 'home AND good AND report AND case', 'home AND good AND report AND case AND number',
                'home OR good', 'home OR good OR report', 'home OR good OR report OR case', 'home OR good OR report OR case OR number']
    iterations = 100
    test_query_factors(queries, iterations=iterations)