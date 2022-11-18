# beeradvocate.com, what kind of raters does the website attract ?

## [I] Abstract
---

We aim to naturally soft-cluster the users within a set of predefined categories. From there, we want to analyse how attractive the website is for each category. Moreover, we aim to uncover the presence/absence of trends in user data per category. Our story is, based solely on user data, we can gain insight about what kind of users a website attracts and whether "natural user soft-clustering"<sup>1</sup> could be a gateway to learn hidden consumer or reviewer behavior.

<sup>1</sup>"natural user soft-clustering": This approach is "cluster together users that satisfy a human-interpretable condition" rather than "cluster together users that are similar based on a similarity metric". Because one user can satisfy many conditions at the same time and thus belong to many categories, the clustering is soft. See examples in section [III].

## [II] Research Questions
---

1. Does natural user soft-clustering gain insight on the attractiveness of a website?
2. Can natural user clustering help uncover hidden consumer/reviewer behavior? What can we tell about the users belonging to the same category? Can we build personas for each category to help the administrators better grasp the kind of users who frequent the website?

## [III] Data

Our working set is the set of files {users.csv, ratings.txt, beers.csv}. Data exploration, analysis and processing of file X is in processing_X.ipynb. We add a layer of processing on the available users.csv and beers.csv. Moreover, we parse ratings.txt to ratings.csv, and process it. Finally, we recompute our own version of beers.csv using the parsed ratings which is integrated with the available data of beers.csv. We prefer storing csv data in parquet format after processing.
Find below the architecture (for reproducibility): "./Data/users.csv", "./Data/ratings.csv", "./Data/beers.csv"

## [IV] Approach
---

We present the mathematical definitions of the most important categories but we will make sure to discuss the validity of the ones we do not mention here with our TA.

Note that:
- $B_u$ is the set of beers the user $u$ has rated
- $\overline{r_b}$ is the average rating of beer $b$
- $r_{u,b}$ is the rating given by user $u$ to beer $b$
- $\sigma_b$ is the standard deviation of the ratings of the beer $b$
- $ref_b$ is a reference of objective rating of a beer $b$. The reference can be either the bros score in the range $[0,5]$, or the BA score scaled down to the range $[0,5]$ (originally in $[0, 100]$). Since the BA score is more abundant (94.5% of beers rated have a BA score), we may be using it as a reference. Both scores are objective ratings provided by administrators of beeradvocate.com who we assume adhere to the guidelines of objective beer reviewing.

**Conformist**: A user $u$ is a conformist if he/she has a high conformism score defined as follows:

$$ CFM_u = \frac{1}{|B_u|}\sum_{b \in B_u}(\frac{r_{u,b}-\overline{r_b}}{\sigma_b})^2$$

The metric is high if the user, on average, rates beers close to the average rating they get. This metric, aside from classifying users, could be an indicator of hearding effect if users from a particular region (a US state) have a high conformism score on average.

**Expert-like**: A user $u$ is an expert-like if he/she has a high score similarity with our reference for expert rating. The score is defined as follows:

$$ EXP_u = \frac{1}{\frac{1}{|B_u|}\sum_{b \in B_u}(\frac{r_{u,b}-ref_b}{\sigma_b})^2}$$

The score is large if, on average, the user rates beers close to the reference score. This metric is our best shot at detecting senior beer raters and experts active on the website based solely on the available data.

**Explorator**: A user $u$ is an explorator if he/she has a high adventurer score defined as follows where $U_{10}(b)$ is the set of at most 10 users that first rated the beer $b$:

$$ XPL_u = \sum_{b \in B_u} \mathbb{1}\{u \in U_{10}(b)\} $$

This metric is larger for users that try out new beers that have not been in the spotlight (that is why they are explorators), and is low for users that almost always rate beers that have been already been rated many times before (in our case 10 times at least). This score provides us with information about which users contribute to enriching the experience on the website, either because they rate beers that do not get much attention, or because they "introduce" new beers on the website by being the first people to rate those beers.

**Adventurer**: A user $u$ is an adventurer if he/she often rates a beer $b$ that has a low score at time $t_{u,b}$ at which he/she rates it, measured as follows where $MAX=5$ is the maximum score a beer can have: 
$$ ADV_u = \sum_{b \in B_u} \mathbb{1}\{r_b(t_{u,b}) < \frac{MAX}{2}\} $$

This metric is higher for users that try out beers that have a bad rating. We want to know if the users frequenting the website are generally reluctant or willing to try out beers with bad ratings.

## [V] Limitations
---

We do not account for the fact that a user could switch categories over time. All users are categorized as in or out of a category once using their total available data.

While we have established via our exploratory data analysis that we can compute the above scores amongst others for almost all users from english speaking countries (US, Canada, England, Australia) knowing that the latter own more than 90% of ratings on beeradvocate.com, we can only approximate the true soft-clustering of these users based on the score distribution of each category independently. However, we believe that, under reasonable assumptions, we can approximate well enough the true classification for the purpose of our usecase. For example, we could consider that all users with an EXP score larger than the sum of the mean and standard deviation of this score are expert-like while the remaining are not.

## [VI] Proposed Timeline
---

- **18 Nov 2022:** Project milestone P2
- **19-25 Nov 2022:** Break from the project to work on Homework 2.
- **26-02 Nov 2022:** 
  - **task 1**: Categorize the users.
  - **task 2**: Extracting per-category information from user data to describe the category (personas). The following tasks are **per-category**:
    - **task 2.1**: statistical description (number of raters in the category, mean rating, ...etc).
    - **task 2.2**: NLP - Sentiment Analysis on reviews.
    - **task 2.3**: NLP - Diversification of vocabulary in reviews.
- **02 Dec 2022:** Homework 2 dealine
- **02-09 Dec 2022:**
  - Continuing with **task 2**:
    - **task 2.4**: Tendencies of raters in the same category. Build per-category personas based on our observations and answer our research question about learning reviewer/consumer behavior from the reviews.
    - **task 2.5 (optional)**: Break down the categories' distributions per state or country.
  - **task 3**: Attractiveness analysis based on when the (categorized) users join the website to answer the corresponding research question.
- **16-23 Dec 2022:**
  - **task 4**: Work on the data story (webpage).
- **23 Dec 2022:** Project milestone P3 

## [VII] Organization
---

  | Full Name | GitHub ID | Task 1 | Task 2.1 | Task 2.2 | Task 2.3 | Task 2.4 | Task 2.5 | Task 3 | Task 4 |
  | :- | :-: | :-: |  :-: |  :-: |  :-: |  :-: |  :-: |  :-: |  :-: |
  | Farouk Boukil | B4Farouk | 0 | 0 | 0 | 1 | 1 | 0 | 1 | 1 | 
  | Henrique Da Silva Gameiro | marluxiaboss | 0 | 0 | 1 | 0 | 1 | 0 | 1 | 1 | 
  | Albin Vernhes | Aco-Hub | 1 | 1 | 0 | 0 | 1 | 0 | 0 | 1 | 
  | Juliette Parchet | jucifer06 | 1 | 0 | 0 | 0 | 1 | 1 | 0 | 1 | 
