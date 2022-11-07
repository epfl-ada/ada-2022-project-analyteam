"""
    Summary
    -------
    This module regroups functions and classes for NLP
"""
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

from transformers import pipeline
    
###################################################################
# SENTIMENT ANALYSIS
###################################################################   

class SentimentAnalyser:
    
    def __init__(self):
        self.__pipeline = pipeline("sentiment-analysis")
    
    def compute(self, text: str):
        assert not(text is None) and len(text) > 0
                
        sentiment = self.__pipeline(text)
        sentiment = sentiment[0] # for a text input, there will be one result in the list
        
        return sentiment["label"], sentiment["score"]

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
        stopwords_ = set(stopwords.words(self.__language))
        tokens = [w for w in self.__tokenizer.tokenize(text) if not(w in stopwords_)]
        return tokens
    
    def lemmatize(self, text):
        lemmas = [self.__lemmatizer.lemmatize(w) for w in self.tokenize(text)]
        return lemmas
        