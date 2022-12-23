# BeerAdvocate.com, Leveraging User Tendencies to Improve User Experience

<a href="https://jucifer06.github.io/ada-website-AnalyTeam/">CLICK HERE TO ACCESS THE DATA STORY</a>

<i>recommendation: use firefox on a computer since we noticed that it has a better display of our website</i>

## [I] Abstract
---

In this project, we soft-cluster the users of beeradvocate.com within a set of predefined categories. Based on that, we analyse how attractive the website is for each category over time. Additionally, we uncover the trends of these categories and build interesting user personas for the website's administrators. By the end, we come up with a few conclusions about what is done right in beeradvocate.com and what could be improved in order to make the website more attractive to the beer lovers community. We also provide a more general result in the form of the conclusion that natural soft-clustering<sup>1</sup> of users can help uncover user groups' behaviors which provides room for improvement in many businesses, including those that heavily rely on recommender systems and providing custom user experiences. We have available to us a dataset of beeradvocate.com data spanning between 1996 and 2017.

<i><sup>1</sup>natural soft-clustering: this approach is "cluster together users that satisfy a human-interpretable condition" rather than "cluster together users that are similar based on a similarity metric". Since one user can satisfy many conditions simulataneously or none at all, any user can belong to any given number of categories including none at all. That is what we mean by natural soft-clustering of users.</i>

## [II] Research Questions
---
1. Can natural soft-clustering as defined above help uncover hidden user tendencies that may be key to improving the user experience on the website?
2. What kind of users does the website attract most and how can we leverage that to improve it?

## [III] Approach
---
### Description:
Our approach is described in depth in our data story  <a href="https://jucifer06.github.io/ada-website-AnalyTeam/">here</a>.

### Limitations:
We do not account for the fact that a user could switch categories over time. All users are categorized in or out of a category once, using their total available data. That is not an issue since we are interested in the overall behavior of each user.

## [IV] Repository Content
---

### Data
In this project, we only use the data about beeradvocate.com provided to use by the team of `CS-401: Applied Data Analysis` at the Swiss Federal Institute of Technology Lausanne, in 2022. Our working set is `users.csv`, `ratings.txt` and `beers.csv`.

### Modules
Below, you will find a description of the usage of each of our modules. We only provide the description of the modules that are relevant to our data story and omit the auxilary ones (particularly `setup.py`):

* `ingestion.py` (milestone 2): used to read the data from CSV or parquet formats and preprocess them on-the-fly.
* `processing.py` (milestone 2): contains the pipelines we use in our processing steps which include dealing with missing values, computing recoverable missing scores using domain specifications, ...etc
* `domain_specs.py` (milestone 2): contains the domain specific knowledge about beer ratings that we used to recover the recoverable missing scores.
* `data_cleaning_reproducibility.py` (milestone 2): contains a single function to run on the raw data to produce our processed data.

* `nlp.py` (milestone 3): used to perform sentiment analysis on user reviews per category.
* `categories.py` (milestone 3): used to compute the clustering scores and to soft-cluster users based on these scores, where the clustering cut-off values are given in our data story.

### Notebooks
Below, you will find a description of the content of each of our notebooks:

* `processing_beers.ipynb` (milestone 2): contains our first analysis of the provided `beers.csv`.
* `processing_ratings.ipynb` (milestone 2): contains our first analysis of the provided `ratings.txt` after parsing it into a CSV file.
* `processing_users.ipynb` (milestone 2): contains our first analysis of the provided `users.csv`.

* `score_distributions.ipynb` (milestone 3): contains the distributions of the scores for this project on all the retained users.
* `statistical_description.ipynb` (milestone 3): statistical descriptions of the score and the categories variation to number of ratings and locations.
* `ratings_reviews_analysis.ipynb` (milestone 3): contains the analysis of the likelihood of users belonging to a category based on the range their number of ratings/reviews fall in.
* `regression_analysis.ipynb` (milestone 3): TODO
* `categories_sentiment.ipynb` (milestone 3): TODO
* `categories_style.ipynb` (milestone 3): TODO
* `attractiveness_time_analysis.ipynb` (milestone 3): contains the time analysis of the attractiveness of the website for the studied categories.

### HTML notebooks
For ease of reading, we have saved our results in the notebooks above in the form of HTML files in the folder `html_notebooks`. Each notebook has a corresponding HTML file of the same name.

## [V] Contribution of Members
---
Find below the constributions of our members since the start of milestone 3:

### Henrique Da Silva Gameiro
* TODO
* data story

### Farouk Boukil
* notebook `attractiveness_time_analysis.ipynb`:
  * analysing what user categories are attracted most by the website over time.
  * analysing sudden big increments in the number of users and trying to relate them to real-life events.
  * analysing the contribution of each category to those big increments of the number of users.
* notebook `score_distributions.ipynb`:
  * analysing the distributions of the scores that we have introduced to get and idea about when to cluster a user within or our of a cathegory.
* notebook `ratings_reviews_analysis.ipynb`: 
  * analysing the likelihood of a user belonging to a category, based on the range his/her number of reviews/ratings falls in.
* data story

### Albin Vernhes
* categories.py
* statistical_description.ipynb 
* data story

### Juliette Parchet
* TODO
* data story
