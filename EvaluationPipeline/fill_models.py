import pandas as pd

from ModelA_WPR.Funcs.create_database import db_insert_dataset as modelA_insert
from ModelA_WPR.Funcs.create_database import db_drop_all_tables as modelA_drop_all
from ModelA_WPR.Funcs.create_database import db_create_tables as modelA_create_all

from ModelB_WVPS.Funcs.create_database import db_insert_dataset as modelB_insert
from ModelB_WVPS.Funcs.create_database import db_drop_all_tables as modelB_drop_all
from ModelB_WVPS.Funcs.create_database import db_create_tables as modelB_create_all

from ModelC_TSV.Funcs.create_database import db_insert_dataset as modelC_insert
from ModelC_TSV.Funcs.create_database import db_drop_all_tables as modelC_drop_all
from ModelC_TSV.Funcs.create_database import db_create_tables as modelC_create_all

from Solr.solr_fill_data import delete_all as solr_reset
from Solr.solr_fill_data import insert_dataset as solr_insert


def fill_modelA(df, pipeline=0):
    modelA_drop_all()
    modelA_create_all()
    modelA_insert(df, pipeline)

def fill_modelB(df, pipeline=0):
    modelB_drop_all()
    modelB_create_all()
    modelB_insert(df, pipeline)

def fill_modelC(df):
    modelC_drop_all()
    modelC_create_all()
    modelC_insert(df)

def fill_solr(df):
    solr_reset()
    solr_insert(df)


if __name__ == "__main__":
    df = pd.read_csv('../../datasets/Arbeitsgruppe/NewsDatasets/news_general_2018_2019_SMALL.csv', header=None)
    df.columns = ['title', 'doc']
    df = df.sample(frac=0.01)
    #fill_modelA(df)
    #fill_modelB(df)
    fill_modelC(df)
    #fill_solr(df)
