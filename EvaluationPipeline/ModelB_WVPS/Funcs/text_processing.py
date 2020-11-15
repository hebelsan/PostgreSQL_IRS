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
import re

nlp = spacy.load('en_core_web_sm', disabel=["tagger", "parser", "ner", "textcat"])
wn.ensure_loaded()
stop = stopwords.words('english')
lemmatizer = WordNetLemmatizer()

def normalize_document(doc, pipeline=0):
    sections = []
    paragraphs = doc.split("\n\n")
    for para in paragraphs:
        if para != "":
            if pipeline == 0:
                sections.append(normalize_section_nltk_fast(para))
            elif pipeline == 1:
                sections.append(normalize_section_spacy(para))
            elif pipeline == 2:
                sections.append(normalize_section_nltk_pos_tag(para))
    return sections


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
    num_words = offset
    return (doc_lexemes, doc_offsets, num_words)


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
    num_words = offset - 1
    return (doc_lexemes, doc_offsets, num_words)


def normalize_section_nltk_pos_tag(text):
    offset = 1 # position value
    doc_lexemes = {}
    doc_offsets = defaultdict(list)
    
    text = re.sub(",|/|u'\u200b'|‘|—|<|>|@|#|:|\n|\"|\[|\]|\(|\)|-|“|”|’|'|\*", " ", text)
    for token, tag in pos_tag(regexp_tokenize(text.lower(), pattern='\w+|\$[\d\.]+|\S+')):
        if token in punctuation or token == " ":
            continue # ignore punctuation
        elif token in stop:
            offset += 1 # increase offset but don't save
        else:
            lemma = ""
            tag = tag[0].lower()
            tag = tag if tag in ['a', 'r', 'n', 'v'] else None
            if not tag:
                lemma = token
            else:
                lemma = lemmatizer.lemmatize(token, tag)
            doc_lexemes[token] = lemma
            doc_offsets[token].append(offset)
            offset += 1
    num_words = offset - 1
    return (doc_lexemes, doc_offsets, num_words)


if __name__ == "__main__":
    TEST_SENTENCE = "/trump >others have called out similarities between the Roys and the Trumps"
    print(TEST_SENTENCE)
    print(normalize_document_fast(TEST_SENTENCE))
    
