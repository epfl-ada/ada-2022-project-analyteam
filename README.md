# beeradvocate.com, what kind of raters does the website attract ?

LINK TO THE DATA STORY: https://jucifer06.github.io/ada-website-AnalyTeam/

## [I] Abstract
---

We aim to naturally soft-cluster the users within a set of predefined categories. From there, we want to analyse how attractive the website is for each category by looking at the cathegory of users it attracts over time. Moreover, we aim to uncover the presence/absence of trends in user data per category. Our story is, based solely on user data, we can gain insight about what kind of users a website attracts and whether "natural user soft-clustering"<sup>1</sup> could be a gateway to learn hidden consumer or reviewer behavior.

<sup>1</sup>"natural user soft-clustering": This approach is "cluster together users that satisfy a human-interpretable condition" rather than "cluster together users that are similar based on a similarity metric". Because one user can satisfy many conditions at the same time and thus belong to many categories, the clustering is soft. See examples in section [IV].

## [II] Research Questions
---

1. Does natural user soft-clustering gain insight on the attractiveness of a website?
2. Can natural user clustering help uncover hidden consumer/reviewer behavior? What can we tell about the users belonging to the same category? Can we build personas for each category to help the administrators better grasp the kind of users who frequent the website?

## [III] Approach
---

Our approach is fully described in our data story, now that it is complete.

## [IV] Soft-Clustering Limitations
---

We do not account for the fact that a user could switch categories over time. All users are categorized in or out of a category once, using their total available data. That is not an issue since we are interested in the overall behavior of each user.

## [V] Repository Content
---

Our working set is the BeerAdvocate files {users.csv, ratings.txt, beers.csv}. Notebook processing_X.ipynb contains the exploratory and descriptive data analysis as well as the data processing we did. To reproduce our processed data directly from the raw data, please run reproducibility.py (may take around 2 hours because of the size of the files) and **make sure you follow the instructions in its docstring**.

## [VI] Contribution of Members
---
Find below the constributions of our members since the start of milestone 3:

### Henrique Da Silva Gameiro
*
*
* data story

### Farouk Boukil
* notebook 'attractiveness_time_analysis.ipynb':
  * analysing what user categories are attracted most by the website over time.
  * analysing sudden big increments in the number of users and trying to relate them to real-life events.
  * analysing the contribution of each category to those big increments of the number of users.
* notebook 'score_distributions.ipynb':
  * analysing the distributions of the scores that we have introduced to get and idea about when to cluster a user within or our of a cathegory.
* notebook 'ratings_reviews_analysis.ipynb': 
  * analysing the likelihood of a user belonging to a category, based on the range his/her number of reviews/ratings falls in.
* data story

### Albin Vernhes
*
*
* data story

### Juliette Parchet
* 
* 
* data story
