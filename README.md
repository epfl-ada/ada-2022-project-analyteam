# BeerAdvocate.com, what kind of raters does the website attract ?

## Abstract
---
We aim to naturally soft-cluster as many users frequenting the website as possible within a set of predefined cathegories based on user data. From there, we want to analyse how attractive the website is for each category over the span of the data collection (2001 - 2017). Moreover, we aim to uncover the presence/absence of trends in user reviews and preferences per category and on a finer granularity (for example, per country or state). The story we want to tell is that, based solely on user data, we can gain insight about what kind of users a website attracts and whether "natural user soft-clustering"<sup>1</sup> could be a gateway to gain insight about hidden consumer or reviewer behavior.

<sup>1</sup>"natural user soft-clustering": users are clustered in a way such that the resulting clusters are interpretable by humans, instead of distance-based clustering seen in class.

## Questions We Want To Address
---
* Does natural user soft-clustering help grasp and quantify the attractiveness of a website for each type of users ? Can we use this clustering to help breakdown and understand variations in the attractiveness of a website over time ?
* Can natural user clustering help uncover hidden consumer/reviewer behavior ? In simpler terms, what can we tell about the users belonging to the same category ? Are they very similar in the language they use or the ratings they give ? Do cross-category preferences in the same location vary significantly ? Can we build personas for each category to help the website administrators better grasp the kind of users who frequent the website ?

## Design choices:
---

We would like to create a category for adventurers users - aka users that try unusual beers.
The idea was to use the number of ratings per beer to compute the popularity of each beer score.
However, there is a problem with this approach. For instance if a user has noted an unpopular beer 5 years ago that has now become popular this user will not be classified has an adventurer. This is against the definition of an adventurer as we define it.
So instead of taking the number of reviews for a beer, we now take the number of review for this beer but in a 2-year time-lapse in order to get a better adventurous score.

One good point would have been to be able to detect user-changing behavior with time. For example, user x starts giving non-conventional rating but with time user x becomes more expert-like. If we analyse user x over all his ratings he will not appear as expert like. However, if we analyze user x in the last year he will clearly emerge as expert like.
To see these variations of user behavior we would have to analyze ratings in different time-lapses. However, we don't have that much rating per user and some time-lapses might be pretty empty because a user simply wasn't rating during this time. 
That is why we've decided to forget about users like user x that varied drastically their behavior with time.

## Proposed timeline
---
- **18 Nov 2022:** Project milestone P2
- **02 Dec 2022:** Homework 2 dealine
- **23 Dec 2022:** Project milestone P3 

## Organization within the team: A list of internal milestones up until project Milestone P3.

## Questions for TAs (optional): Add here any questions you have for us related to the proposed project.
