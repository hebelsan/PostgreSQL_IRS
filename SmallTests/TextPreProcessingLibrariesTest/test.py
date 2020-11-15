import pandas as pd
import spacy
import nltk
from nltk import word_tokenize
from nltk import pos_tag
from nltk.tokenize.regexp import regexp_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet as wn
from string import punctuation
from collections import defaultdict
from textblob import TextBlob
import re
import time

TEST_SENTENCE = "This is a test sentence. It includes contractions like I'm and he's or Paul's house. Stop words and Punctuations like , ' ' ! ? should be removed. Let's see how Named Entities like Google are handled"

nlp = spacy.load('en_core_web_sm', disabel=["tagger", "parser", "ner", "textcat"])
wn.ensure_loaded()
stop = stopwords.words('english')
lemmatizer = WordNetLemmatizer()
num_words = 0

def count_words(text):
    global num_words
    text = re.sub(",|/|u'\u200b'|‘|—|<|>|@|#|:|\n|\"|\[|\]|\(|\)|-|“|”|’|'|\*", " ", text)
    for token in regexp_tokenize(text.lower(), pattern='\w+|\$[\d\.]+|\S+'):
        if token in punctuation or token == " ":
            continue # ignore punctuation
        elif token in stop:
            num_words += 1
        else:
            num_words += 1


def normalize_section_spacy(text):
    offset = 1 # position value
    doc_lexemes = {}
    doc_offsets = defaultdict(list)

    text = re.sub(",|/|u'\u200b'|‘|—|<|>|@|#|:|\n|\"|\[|\]|\(|\)|-|“|”|’|'|\*", ' ', text)
    text = nlp(text) # tokenize the text
    for token in text:
        if token.text in punctuation or token.text == " ":
            continue # ignore punctuation
        elif token.is_stop or token.like_url or token.like_email:
            offset += 1 # increase offset but don't save
        else:
            doc_lexemes[token.text.lower()] = token.lemma_.lower()
            doc_offsets[token.text.lower()].append(offset)
            offset += 1


def normalize_section_nltk_fast(text):
    offset = 1 # position value
    doc_lexemes = {}
    doc_offsets = defaultdict(list)
    
    text = re.sub(",|/|u'\u200b'|‘|—|<|>|@|#|:|\n|\"|\[|\]|\(|\)|-|“|”|’|'|\*", " ", text)
    for token in regexp_tokenize(text.lower(), pattern='\w+|\$[\d\.]+|\S+'):
        if token in punctuation or token == " ":
            continue # ignore punctuation
        elif token in stop:
            offset += 1 # increase offset but don't save
        else:
            doc_lexemes[token] = lemmatizer.lemmatize(token)
            doc_offsets[token].append(offset)
            offset += 1


def normalize_section_textblob(text):
    offset = 1 # position value
    doc_lexemes = {}
    doc_offsets = defaultdict(list)
    
    text = re.sub(",|/|u'\u200b'|‘|—|<|>|@|#|:|\n|\"|\[|\]|\(|\)|-|“|”|’|'|\*", " ", text)
    text = TextBlob(text)
    for token in text.words:
        if token in punctuation or token == " ":
            continue # ignore punctuation
        elif token in stop:
            offset += 1 # increase offset but don't save
        else:
            doc_lexemes[token] = token.lemmatize()
            doc_offsets[token].append(offset)
            offset += 1



def normalize_dataset(percentage, df):
    global num_words
    num_words = 0
    df = df.sample(frac=percentage)
    df.apply(lambda x: count_words(x['doc']), axis=1)
    print("num words: " + str(num_words))
    start = time.time()
    df.apply(lambda x: normalize_section_spacy(x['doc']), axis=1)
    print("timeSpacy: " + str(time.time() - start))
    start = time.time()
    df.apply(lambda x: normalize_section_nltk_fast(x['doc']), axis=1)
    print("timeNLTK: " + str(time.time() - start))
    start = time.time()
    df.apply(lambda x: normalize_section_textblob(x['doc']), axis=1)
    print("textblob: " + str(time.time() - start))

if __name__ == "__main__":
    df = pd.read_csv('../../../datasets/Arbeitsgruppe/NewsDatasets/news_general_2018_2019_SMALL.csv', header=None, skiprows=1)
    df.columns = ['title', 'doc']
    
    percentages = [0.000005, 0.00001, 0.00005, 0.0001, 0.001, 0.005]
    for perc in percentages:
        normalize_dataset(perc, df)
