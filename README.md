# AnalyTeam - Beer Reviews Dataset

## Title: Beer reviewers Categorization 

## Abstract: (A 150 word description of the project idea and goals. What’s the motivation behind your project? What story would you like to tell, and why?)
Can we categorize user behavior/personalities with meaningful boundaries and extract trends from these groups? Using the beer review dataset, we want to extract relevant features in order to catagorize the users into some personnality classes, like *Adventurer*, *Free-thinker*, *Conformist*, *Beer-lover*, *Hard-to-please*,*Like the expert* ... 

## Research Questions: A list of research questions you would like to address during the project.
* How do we classify users?
* Why is this classification backed up by data?
* Why is this classification useful to understand the different types of user/useful for the site to categorize them? For example if a reviewer always give really higher rating than the average indicate it with a badge on their review called “Beer-lover”.
* What trends can we extract from these different classification of users? 

## Design choices:

We would have liked to create a category for adventurers users aka users that try unusual beers.
The idea was to use the number of rating per beer in order to compute the popularity of each bier score.
However there is a problem with this approche. Imagine a user has noted an unpopular beers 5 years ago that has now become popular this user will not be classified has adventurer. This is against the definition of adventurer has we've thought about it.
So instead of taking the number of review for a beer we now take the number of review for this bier but in a 2 year timelaps in order to get a better adventurous score.

One good point would have been to be able to detect user changing behavior with time. For exemple user x start giving none conventional rating but with time user x become more expert like. If we analyse user x over all his ratings he will no appear as expert like. However if we analyse user x in the last year he will clearly appear as expert like.
In order to see this variations of user behavior we would have to analyse ratings in diffrent timelaps however we don't have that much rating per user and some timelaps might be quite empty because a user simply wasn't rating during this time. 
That's why we've decided to forget about user like user x that varied drastically theyr behavior with time.

## Proposed timeline

## Organization within the team: A list of internal milestones up until project Milestone P3.

## Questions for TAs (optional): Add here any questions you have for us related to the proposed project.
