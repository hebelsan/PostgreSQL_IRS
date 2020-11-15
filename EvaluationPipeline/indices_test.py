import re
from functools import partial

from query_speed import *
from analyse_size import *

from ModelA_WPR.Funcs.indices import *
from ModelB_WVPS.Funcs.indices import *


def print_analysis_speed_a(query, iterations=20, withOrderLim=True, withExtension=False):
    analyse_query_time_modela(query, iterations=iterations, limit=10, withOrderLim=withOrderLim, withExtension=withExtension)
    size = index_size_modela(sum=True) / (1024*1024)
    print("size: " + str(size) +" MB")


def print_analysis_speed_b(query, iterations=20, withOrderLim=True, withExtension=False):
    analyse_query_time_modelb(query, iterations=iterations, limit=10, withOrderLim=withOrderLim, withExtension=withExtension)
    size = index_size_modelb(sum=True) / (1024*1024)
    print("size: " + str(size) +" MB")


def analyse_query_mod_a(query, iterations=20, withOrderLim=True, withExtension=False):
    modelA_add_index_brin()
    print("*******STANDARD*******")
    explain_query_modela(query, limit=10, withOrderLim=withOrderLim)


##
#   Mod A
##
def analyse_speed_indice_mod_a(query, iterations=20, withOrderLim=True, withExtension=False):
    # multi all
    print("multi column ALL")
    modelA_add_multi_all()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    print("Index Only ALL")
    modelA_add_indx_only_all()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    ## Standard
    print("standard Slow")
    modelA_add_standard_slow()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    print("standard")
    modelA_add_standard()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    ## Index Only
    print("index only")
    modelA_add_indx_only()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    ## Multi
    print("multicolumn only")
    modelA_add_multi()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)
    ## BRIN
    print("Brin 128")
    modelA_add_index_brin()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    print("Brin 32")
    modelA_add_index_brin_32()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)
    
    print("Brin 16")
    modelA_add_index_brin_16()
    print_analysis_speed_a(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)


##
#   Mod B
##
def analyse_speed_indice_mod_b(query, iterations=20, withOrderLim=True, withExtension=False):
    # standard slow
    print("Standard Slow")
    modelB_add_indx_only()
    print_analysis_speed_b(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    # standard
    print("Standard")
    modelB_add_standard()
    print_analysis_speed_b(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    # gist index only
    print("Index Only")
    modelB_add_indx_only()
    print_analysis_speed_b(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    # gist small
    print("Gist small")
    modelB_add_gist_normal()
    print_analysis_speed_b(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)

    # gist big
    print("Gist big")
    modelB_add_gist_big()
    print_analysis_speed_b(query, iterations=iterations, withOrderLim=withOrderLim, withExtension=withExtension)


if __name__ == "__main__":
    iterations = 100
    query = 'president'

    '''
    analyse_speed_indice_mod_a(query, iterations=iterations, withOrderLim=True, withExtension=False)
    print("\n\n\n\n *********QUERY: trump and presiden**************")
    analyse_speed_indice_mod_a('trump AND president', iterations=iterations, withOrderLim=True, withExtension=True)
    print("\n\n\n\n  *********QUERY: trump or presiden**************")
    analyse_speed_indice_mod_a('trump OR president', iterations=iterations, withOrderLim=True, withExtension=True)
    '''

    analyse_speed_indice_mod_b(query, iterations=iterations, withOrderLim=True, withExtension=False)