# K-means clustering
Project in Cornell CS4320 to implement k-means clustering algorithm using Hadoop map reduce.

# Authors
Giri Kuncoro (gk256@cornell.edu), Batu Inal (bi49@cornell.edu)

# Description
The k-means clustering algorithm is implemented based on below idea:
1. Pick k points to serve as the initial cluster centroids
2. For every point Pk, find the closes centroid Ci (using Euclidean distance) and associate it with Ci
3. Update the Ci's by taking all points associated with each Ci in the previous step and setting the new Ci's to the mean of the points associated with it
4. Repeat for a specific number of iterations or until the centroids stop changing, whichever comes first

# Acknowledgments
The code is implemented based on the algorithm provided in Homework 4 CS4320 instruction and Hadoop documentation, particularly WritableComparator, Mapper and Reducer class sections.
