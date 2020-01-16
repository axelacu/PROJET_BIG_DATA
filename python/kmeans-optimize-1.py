# -*- coding: utf-8 -*-


from pyspark import SparkContext, SparkConf
from math import sqrt
import time

def computeDistance(x,y):
    return sqrt(sum([(a - b)**2 for a,b in zip(x,y)]))


def closestCluster(dist_list):
    cluster = dist_list[0][0]
    min_dist = dist_list[0][1]
    for elem in dist_list:
        if elem[1] < min_dist:
            cluster = elem[0]
            min_dist = elem[1]
    return (cluster,min_dist)

def sumList(x,y):
    return [x[i]+y[i] for i in range(len(x))]

def moyenneList(x,n):
    return [x[i]/n for i in range(len(x))]

def simpleKmeans(data, nb_clusters):
    clusteringDone = False
    number_of_steps = 0
    current_error = float("inf")
    # A broadcast value is sent to and saved  by each executor for further use
    # instead of being sent to each executor when needed.
    nb_elem = sc.broadcast(data.count())

    #############################
    # Select initial centroides #
    #############################

    centroides = sc.parallelize(data.takeSample('withoutReplacment',nb_clusters))\
              .zipWithIndex()\
              .map(lambda x: (x[1],x[0][1][:-1]))
    # (0, [4.4, 3.0, 1.3, 0.2])
    # In the same manner, zipWithIndex gives an id to each cluster

    while not clusteringDone:
        start = time.time()
        #############################
        # Assign points to clusters #
        #############################

        joined = data.cartesian(centroides)
        # ((0, [5.1, 3.5, 1.4, 0.2, 'Iris-setosa']), (0, [4.4, 3.0, 1.3, 0.2]))

        # We compute the distance between the points and each cluster
        dist = joined.map(lambda x: (x[0][0],(x[1][0], computeDistance(x[0][1][:-1], x[1][1]))))
        # (0, (0, 0.866025403784438))

        dist_list = dist.groupByKey().mapValues(list)
        # (0, [(0, 0.866025403784438), (1, 3.7), (2, 0.5385164807134504)])

        # We keep only the closest cluster to each point.
        min_dist = dist_list.mapValues(closestCluster)
        # (0, (2, 0.5385164807134504))

        # assignment will be our return value : It contains the datapoint,
        # the id of the closest cluster and the distance of the point to the centroid
        assignment = min_dist.join(data)

        # (0, ((2, 0.5385164807134504), [5.1, 3.5, 1.4, 0.2, 'Iris-setosa']))

        ############################################
        # Compute the new centroid of each cluster #
        ############################################

        clusters = assignment.map(lambda x: (x[1][0][0], x[1][1][:-1]))
        # (2, [5.1, 3.5, 1.4, 0.2])

        count = clusters.map(lambda x: (x[0],1)).reduceByKey(lambda x,y: x+y)
        somme = clusters.reduceByKey(sumList)
        centroidesCluster = somme.join(count).map(lambda x : (x[0],moyenneList(x[1][0],x[1][1])))

        ############################
        # Is the clustering over ? #
        ############################

        # Let's see how many points have switched clusters.
        if number_of_steps > 0:
            switch = prev_assignment.join(min_dist).filter(lambda x: x[1][0][0] != x[1][1][0]).count()
        else:
            switch = 150
        if switch == 0 or number_of_steps == 100:
            clusteringDone = True
            error = sqrt(min_dist.map(lambda x: x[1][1]).reduce(lambda x,y: x + y))/nb_elem.value
            end = time.time()
            time_taken = end - start
            print('Time last iter  ', number_of_steps, ' : ', time_taken)
        else:
            centroides = centroidesCluster
            prev_assignment = min_dist
            end = time.time()
            time_taken = end - start
            print('Time iteration numero  ', number_of_steps, ' : ', time_taken)
            number_of_steps += 1

    return (assignment, error, number_of_steps)

path_source_local = "file:////home/acuna/Projets/PROJET_BIG_DATA/Repository/data/iris/iris.data.txt"
path_source_dfs = "hdfs:/user/user87/projet-bd/data/iris/iris.data.txt"
path_dest_local = "file:////home/acuna/Projets/PROJET_BIG_DATA/Repository/output-local-test"
path_dest_dfs = "hdfs:/user/user87/projet-bd/output/iris/iris-many-optimize-1"

if __name__ == "__main__":
    num_of_partition = 12
    conf = SparkConf().set("spark.default.parallelism", num_of_partition).setAppName('exercice')
    sc = SparkContext(conf=conf)

    lines = sc.textFile(path_source_dfs)
    data = lines.map(lambda x: x.split(','))\
            .map(lambda x: [float(i) for i in x[:4]]+[x[4]])\
            .zipWithIndex()\
            .map(lambda x: (x[1],x[0]))
    # zipWithIndex allows us to give a specific index to each point
    # (0, [5.1, 3.5, 1.4, 0.2, 'Iris-setosa'])

    clustering = simpleKmeans(data,3)
    clustering[0].saveAsTextFile(path_dest_dfs)
    
    # if you want to have only 1 file as a result, then:
    #clustering[0].coalesce(1).saveAsTextFile("hdfs:/user/user87/projet-bd/output/iris/iris-many-files")

    print (clustering)
