#!/usr/bin/env python3

#wget https://msmarco.blob.core.windows.net/msmarcoranking/msmarco-docs.tsv.gz
#gzip -d msmarco-docs.tsv.gz

import numpy as np
import dask.dataframe as dd
import spacy
from string import punctuation

nlp = spacy.load('en_core_web_sm', disabel=["tagger", "parser", "ner"])

def normalize_section(text):
    offset = 1 # position value
    sec_vocab = [] # vocabulary of the section
    
    text = nlp(text) # tokenize the text
    for token in text:
        if token.text in punctuation:
            continue # ignore punctuation
        elif token.is_stop or token.like_url or token.like_email:
            offset += 1 # increase offset but don't save
        else:
            if token.ent_type != 0: # is entity
                sec_vocab.append({'orig':token.text,
                                  'lex':token.text,
                                  'pos':offset})
            if token.ent_type == 0: # is no entity
                sec_vocab.append({'orig':token.text.lower(),
                                  'lex':token.lemma_.lower(),
                                  'pos':offset})
            offset += 1
    print(sec_vocab)
    return sec_vocab



def main():
    text = "This is just a test U.K."
    #vocab = normalize_section(text)
    tokens = nlp(text)
    for token in tokens:
        print(token.text)
    
    '''
    df=dd.read_table('../../datasets/TREC/DeepLearningTREC2019/collection.tsv',blocksize=100e6,header=None)
    df.columns=['pid', 'passage']
    test_dataset = df.sample(frac=0.001)
    
    print(test_dataset.head())
    test_dataset['passage'].map(lambda x: normalize_section(x), meta=('passage', 'object')).compute()
    '''



if __name__ == "__main__":
    main()
