Wikipedia category graph loader
========================

An implementation of the algorithm of the article "Automatically assigning Wikipedia articles to macro-categories".

It loads the wikipedia category graph in a Neo4j embedded instance, then proceed to calculate the distance of each category from a set of chosen ones.

The process follows two steps:

1. load the file caterogy.sql obtained from Wikimedia periodical database exports, creating a node with ID and name properties for each category
2. load the file categorylinks.sql to create edges between categories and articles (creating article nodes on the fly)
3. calculate the distance from the chosen categories with the algorithm explained in the paper, using a different cost for edges depending on the travelling direction

The program can be used for any wikipedia edition, for en.wikipedia it took about 20 hours on my laptop and generated a 15GB graph database instance, including Lucene indexes.
