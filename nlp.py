"""
    Summary
    -------
    This module regroups functions and classes for NLP
"""
# NLTK
import nltk
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

# TRANSFORMERS
from transformers import pipeline

# OTHER
from typing import Set, List

###################################################################
# NLTK SETUP
###################################################################

def nltk_init():
    nltk.download("stopwords")
    
###################################################################
# SENTIMENT ANALYSIS
###################################################################   

class SentimentAnalyser:
    
    def __init__(self):
        self.__pipeline = pipeline("sentiment-analysis")
    
    def scores(self, text: str):
        assert not(text is None)
        
        rates_ = self.__pipeline(rates_)
        return {'+': rates_[0]["score"], '-': rates_[1]["score"]}

###################################################################
# TOKENIZER
###################################################################

class Tokenizer:
    
    def __init__(self, language):
        self.__tokenizer = RegexpTokenizer(r'\w+')
        self.__lemmatizer = WordNetLemmatizer()
        self.__language  = language
            
    def tokenize(self, text):
        text = text.lower()
        tokens = [w for w in self.__tokenizer.tokenize(text) if w not in stopwords.words(self.__language)]
        return tokens
    
    def lemmatize(self, text):
        tokens = self.__tokenizer.tokenize(text)
        lemmas = [self.__lemmatizer.lemmatize(w) for w in tokens]
        return lemmas
        