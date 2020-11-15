import pandas as pd
import threading
from analyse_dataset import analyse_dataset
from fill_models import *
from validate_models import *
from analyse_size import *

from ModelA_WPR.Funcs.add_psql_functions import add_psql_functions as modelA_add_psql_funcs
from ModelB_WVPS.Funcs.add_psql_functions import add_psql_functions as modelB_add_psql_funcs
from ModelC_TSV.Funcs.add_psql_functions import add_psql_functions as modelC_add_psql_funcs

import time


def evaluation_pipeline(file_path, frac=0.01, type='csv'):
    ##
    # Create PSQL functions
    ##
    modelA_add_psql_funcs()
    modelB_add_psql_funcs()
    modelC_add_psql_funcs()
    
    if not is_solr_up():
        print('start the solr server first...')
        return
    
    query = input('query to validate correctness (e.g. trump): ')
    pipeline = int(input('nlp pipeline fast(nltk)=0 or slow(spacy)=1 slow(nltk+pos_tag)=2: '))
    
    ##
    # create dataframe
    ##
    df = None
    if type == 'csv':
        df = pd.read_csv(file_path, header=None, skiprows=1)
        df.columns = ['title', 'doc']
    df = df.sample(frac=frac)
    
    ##
    # analyse dataset
    ##
    anlyse_thread = threading.Thread(target=analyse_dataset, args=(df['doc'], file_path), daemon=True)
    anlyse_thread.start()
    
    ##
    # fill models
    ##
    first = time.time()
    fill_moda_thread = threading.Thread(target=fill_modelA, args=(df, pipeline), daemon=True)
    fill_moda_thread.start()
    
    fill_modb_thread = threading.Thread(target=fill_modelB, args=(df, pipeline), daemon=True)
    fill_modb_thread.start()
    
    fill_modc_thread = threading.Thread(target=fill_modelC, args=(df, ), daemon=True)
    fill_modc_thread.start()
    
    fill_solr_thread = threading.Thread(target=fill_solr, args=(df, ), daemon=True)
    fill_solr_thread.start()
    
    # Wait until all threads are finished
    anlyse_thread.join()
    fill_moda_thread.join()
    fill_modb_thread.join()
    fill_modc_thread.join()
    fill_solr_thread.join()
    
    print("time to fill models:" + str(time.time() - first))
    
    ##
    # Check Correctness
    ##
    print("\n *****Correctness check*****")
    check_num_sections()
    check_num_results_query(query)
    check_ranking_results(query)
    
    ##
    # Analyse size of models
    ##
    size_databases()
    table_sizes_modela()
    table_sizes_modelb()
    table_sizes_modelc()
    
    ##
    # Performance boost of modela and modelb
    ##
    
    
    ##
    # Analyse results of ranking
    ##
    
    
    ##
    # Analyse speed of ranking
    ##


if __name__ == "__main__":
    #evaluation_pipeline('../../datasets/Correctness/test.csv', frac=1.0)
    evaluation_pipeline('../../datasets/Arbeitsgruppe/NewsDatasets/news_general_2018_2019_SMALL.csv', frac=1.0)
    
