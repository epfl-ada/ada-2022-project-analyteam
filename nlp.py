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

import transformers as trafos

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
        # text that is too long (its tokenization length exceeds the model's limit)
        # will be truncated
        
        # use the GPU if available else use the CPU
        device = 0 if trafos.is_torch_available() else -1
        print("TEXTS")
        print(texts[0])
        #dataset = RatingsDataset(texts)
        #dataloader = DataLoader(dataset, batch_size=32, shuffle=False)
        ds = 
        print("DATASET")
        print(dataset[0])
        sentiments = np.array([])
        for batch in tqdm(dataloader):
            if cuda.is_available():
                batch = batch.cuda()
            sentiments = np.append(sentiments, self.__pipeline(batch, truncation=True))
        #sentiments = self.__pipeline(texts, device, truncation=True)

        return [(sentiment["label"], sentiment["score"]) for sentiment in sentiments]

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
        