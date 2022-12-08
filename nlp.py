"""
    Summary
    -------
    This module regroups functions and classes for NLP
"""
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from torch.utils.data import Dataset
from torch.utils.data import DataLoader
from tqdm.auto import tqdm
import numpy as np

#HuggingFace
from datasets import Dataset
from transformers.pipelines.pt_utils import KeyDataset
import transformers as trafos

#Vader
import vaderSentiment
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

#NLP libraries
import spacy, nltk, gensim, sklearn

import pandas as pd



class RatingsDataset(Dataset):
    def __init__(self, ratings):
        self.ratings = ratings
    def __len__(self):
        return len(self.ratings)
    def __getitem__(self, item):
        return self.ratings[item]

    


    
###################################################################
# SENTIMENT ANALYSIS
###################################################################   

class SentimentAnalyser:
    """
    Wrapper class. It wraps a Transformer for sentiment analysis.
    """
    def __init__(self):
        self.__pipeline = trafos.pipeline(
            model="distilbert-base-uncased-finetuned-sst-2-english",
            revision="af0f99b",
            tokenizer=trafos.DistilBertTokenizerFast.from_pretrained(
                "distilbert-base-uncased"),
            device=0
            )# device=0 to use GPU
                                   
    def compute(self, text: str):
        """
        Computes the label and score of sentiment analysis on a string input.
        
        Args:
            text (str): the input.
        
        Returns:
            (Tuple[str, float]): the label "POSITIVE" or "NEGATIVE" and the corresponding score. 
        """
        assert not(text is None) and len(text) > 0
        # text that is too long (its tokenization length exceeds the model's limit)
        # will be truncated
        sentiment = self.__pipeline(text, truncation=True, device=0) # device=0 to use GPU
        sentiment = sentiment[0] # for a text input, there will be one result in the list
        
        return sentiment["label"], sentiment["score"]
    
    def batch_compute(self, texts):
        """
        Computes the label and score of sentiment analysis on a list of string inputs.
        
        Args:
            texts (List[str]): the list of inputs.
        
        Returns:
            (List[Tuple[str, float]]): the list of labels "POSITIVE" or "NEGATIVE" and the corresponding scores. 
        """
        assert not(texts is None) 

        
        # use the GPU if available else use the CPU
        device = 0 if trafos.is_torch_available() else -1

        # create a new dataframes with the texts
        df = pd.DataFrame({"text": texts})

        # create a dataset from the dataframe
        dataset = Dataset.from_pandas(df)
        sentiments = np.array([])

        # process the ratings in batches, text that is too long (its tokenization length exceeds the model's limit)
        # will be truncated
        for out in tqdm(self.__pipeline(KeyDataset(dataset, "text"), truncation=True, batch_size=32)):
            np.append(sentiments, out)
        return [(sentiment["label"], sentiment["score"]) for sentiment in sentiments]


class VaderSentimentAnalyser:

    def __init__(self):
        self.nlp_pipeline = spacy.load('en_core_web_sm')
        self.nlp_pipeline.remove_pipe('parser')
        #self.nlp_pipeline.remove_pipe('tagger')
        #self.nlp_pipeline.add_pipe('morphologizer')
        self.nlp_pipeline.add_pipe('sentencizer')
        self.vader_analyzer = SentimentIntensityAnalyzer()
    

    def compute(self, text):
        compound_sent = 0

        # get full text of the document
        processed_texts = self.nlp_pipeline(text)
        doc = processed_texts.doc

        # compute average compound sentiment for the document
        nb_sents = 0
        for sent in doc.sents:
            compound_sent += self.vader_analyzer.polarity_scores(str(sent))['compound']
            nb_sents += 1
        
        compound_sent /= nb_sents
        

        #compound_sent = self.vader_analyzer.polarity_scores(str(doc))['compound']

        print("done1")

        print("compound_sent")
        print(compound_sent)

        return  compound_sent



    def compute_batch(self, texts):
        negative_sent = []
        postive_sent = []
        compound_sent = []

        #processed_texts = self.nlp_pipeline.pipe(texts)
        for doc in tqdm(self.nlp_pipeline.pipe(texts, n_process=-1, batch_size=1000)):
            sentences = [sent.string.strip() for sent in doc.sents]
            for sentence in sentences:
                vs = self.vader_analyzer.polarity_scores(sentence)
                negative_sent.append(vs['neg'])
                postive_sent.append(vs['pos'])
                compound_sent.append(vs['compound'])

        print("processing texts")
        print(processed_texts.sents[0])
        print(processed_texts.sents[0])
        negative_sent = [negative_sent.append(self.vader_analyzer.polarity_scores(sent.text)['neg']) for sent in processed_texts.sents]
        postive_sent = [postive_sent.append(self.vader_analyzer.polarity_scores(sent.text)['pos']) for sent in processed_texts.sents]
        compound_sent = [compound_sent.append(self.vader_analyzer.polarity_scores(sent.text)['compound']) for sent in processed_texts.sents]

        return negative_sent, postive_sent, compound_sent



###################################################################
# TOKENIZER
###################################################################

class Tokenizer:
    """
    Text Tokenizer used to tokenize strings for the purpose of vocabulary comparison.
    """
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
        