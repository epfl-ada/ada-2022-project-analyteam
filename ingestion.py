"""
Summary
-------
    This module implements functions and classes to ingest data.

Author
------
    Farouk Boukil
    Juliette Parchet
"""

# DASK
import dask.dataframe as dd

# LOCAL
from nlp import SentimentAnalyser, Tokenizer

# OTHERS
import csv
from typing import List

###################################################################
# GLOBALS
###################################################################

__ENCODING = "utf-8"

__REVIEW_TAG = "text"
__REVIEW_PRESENCE_TAG = "review"

###################################################################
# CSV READERS
# need dask: call dask_init first, then dask_shutdown when done
###################################################################

def read_csv_lazy(path: str, keepcols: List = None, **kwargs):
    """
    Reads columns from a CSV file into a Dask Dataframe.

    Args:
        path (str): 
            The path to the CSV file.
        keepcols (List[int] or List[str]): 
            Columns to be kept. Refer to the "usecols" attribute of pandas.Dataframe. 
            Defaults to None, in which case no column is discarded.
            The behavior on an empty list is the same as that on None.

    Returns:
        (dask.dataframe.Dataframe): The resulting dataframe.
    """
    # if no column is specified, keep all
    keep_all = keepcols is None or len(keepcols) == 0

    lazy_df = dd.read_csv(urlpath=path) if keep_all \
        else dd.read_csv(urlpath=path, usecols=keepcols)

    return lazy_df

def read_csv(path: str, keepcols: List = None, **kwargs):
    """
    Reads columns from a CSV file into a pandas Dataframe.

    Args:
        path (str): 
            The path to the CSV file.
        keepcols (List[int] or List[str]): 
            Columns to be kept. Refer to the "usecols" attribute of pandas.Dataframe. 
            Defaults to None, in which case no column is discarded.
            The behavior on an empty list is the same as that on None.

    Returns:
        (pandas.Dataframe): The resulting dataframe.
    """
    return read_csv_lazy(path, keepcols, **kwargs).compute()

###################################################################
# PARSERS
###################################################################

def __rating_vals_from(
    rating_lines      : List[str], 
    selected_tags     : List[str],
    tokenizer         : Tokenizer,
    sentiment_analyser: SentimentAnalyser):
    """_summary_

    Args:
        rating_lines (List[str]): _description_
        selected_tags (List[str]): _description_
        tokenizer (Tokenizer): _description_
        sentiment_analyser (SentimentAnalyser): _description_

    Returns:
        _type_: _description_
    """
    # assumes that every line in rating_lines list has the format tag:value
    
    rating = []
        
    has_review = False
    review = None
    
    # getting the rating's attributes of interest
    sep = ":"
    for line in rating_lines:
        line_split = line.split(sep)
        tag, value = line_split[0], sep.join(line_split[1:])
        
        if tag in selected_tags:
            rating.append(value)
        if tag == __REVIEW_TAG:
            review = value
        elif tag == __REVIEW_PRESENCE_TAG:
            has_review = bool(value)
    
    # review lemmatization
    if __REVIEW_TAG in selected_tags:
        if has_review:
            lemmas = tokenizer.lemmatize(review)
            rating.append(lemmas)
        else:
            rating.append("")   
    
    # sentiment analysis
    scores = sentiment_analyser.scores(review)    
    rating.append(scores['+'] if has_review else 0)
    rating.append(scores['-'] if has_review else 0)
    
    return rating

def __next_rating(file):
    """_summary_

    Args:
        file (_type_): _description_

    Returns:
        _type_: _description_
    """
    # <> assumes that different ratings are spaced by at least one "\n"
    # but not necessarily exactly one "\n"
    # <> assumes that no comment has a "\n" in it
    rating_lines = []
    next_line = file.readline()
    while len(next_line) >= 1:
        if len(next_line) == 1:
            next_line = file.readline() 
            continue
        rating_lines.append(next_line.strip())
        next_line = file.readline()
    return rating_lines

def txt2csv(
    from_path      : str,
    to_path        : str,
    selected_tags  : List[str], 
    all_tags       : List[str]):
    """_summary_

    Args:
        from_path (str): _description_
        to_path (str): _description_
        selected_tags (List[str]): _description_
        all_tags (List[str]): _description_
        sentiment_sets (Tuple[List[str], List[str]]): _description_
    """

    assert not(to_path is None) and len(to_path) > 0
    assert not(from_path is None) and len(from_path) > 0    
    assert not(all_tags is None) and len(all_tags) > 0
    
    # select all tags by default
    selected_tags = selected_tags if not(selected_tags is None) and len(selected_tags) > 0 \
        else all_tags
    
    with open(from_path, 'r', encoding=__ENCODING) as txt_file,\
        open(to_path, 'w', encoding=__ENCODING, newline='') as csv_file:
        
        writer = csv.writer(csv_file)
        writer.writerow(selected_tags)
        
        tokenizer = Tokenizer(language="english")
        sentiment_analyser = SentimentAnalyser()
        
        rating_lines = __next_rating(txt_file)
        while len(rating_lines) > 0:
            rating = __rating_vals_from(rating_lines, selected_tags, tokenizer, sentiment_analyser)         
            writer.writerow(rating)
            rating_lines = __next_rating(txt_file)
