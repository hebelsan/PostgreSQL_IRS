import nltk
from nltk import word_tokenize
from nltk import pos_tag
from nltk.tokenize.regexp import regexp_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn

from string import punctuation
import re
import pandas as pd
from pathlib import Path

NUM_DOCS = 0
DATASET_SIZE_GB = 0

NUM_SECS = 0
NUM_SENTENCES = 0
NUM_WORDS = 0
NUM_STOP_WORD = 0

AVG_TERMS_PER_SEC = 0
AVG_LEXEMES_PER_SEC = 0
AVG_STOP_WORD_PER_SEC = 0

wn.ensure_loaded()
stop = stopwords.words('english')
lemmatizer = WordNetLemmatizer()

def analyse(text):
    global NUM_SECS
    global NUM_SENTENCES
    global NUM_WORDS
    global NUM_STOP_WORD
    NUM_SECS += len(text.split("\n\n"))
    
    text = re.sub('@|#|:|\n|-|’', ' ', text)
    tokens = regexp_tokenize(text.lower().replace("'", " "), pattern='\w+|\$[\d\.]+|\S+')
    
    terms = set()
    lexemes = set()
    for token in tokens:
        if token in punctuation or token == " ":
            if token in ['!', '.', '?']:
                NUM_SENTENCES += 1
            continue # ignore punctuation
        elif token in stop:
            NUM_WORDS += 1
            NUM_STOP_WORD += 1
        else:
            NUM_WORDS += 1
            terms.add(token)
            lexemes.add(lemmatizer.lemmatize(token))
            

def analyse_dataset(df_docs, file_path):
    NUM_DOCS = df_docs.size
    DATASET_SIZE_GB = Path(file_path).stat().st_size / 1000000
    df_docs.map(lambda x: analyse(x))

    print("\n*****ANALYSE DATASET RESULTS*****")
    print("documents: " + str(NUM_DOCS))
    print("sections: " + str(NUM_SECS))
    print("sentences: " + str(NUM_SENTENCES))
    print("total words (with stop words): " + str(NUM_WORDS))
    print("total stop words: " + str(NUM_STOP_WORD))
    
    print("AVG total words per sec: " + str(NUM_WORDS // NUM_SECS))
    print("AVG stop words per sec: " + str(NUM_STOP_WORD // NUM_SECS))
    print("AVG sentence per sec: " + str(NUM_SENTENCES // NUM_SECS))
    print()

if __name__ == "__main__":
    file_path = '../../datasets/Arbeitsgruppe/NewsDatasets/news_general_2018_2019_SMALL.csv'
    df = pd.read_csv(file_path, header=None)
    df.columns = ['title', 'doc']
    df = df.sample(n=10000)
    analyse_dataset(df['doc'], file_path)
