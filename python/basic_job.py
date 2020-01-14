from pyspark import SparkContext, SparkConf
from math import sqrt

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

path_source_local = "file:////home/acuna/Projets/PROJET_BIG_DATA/Repository/data/iris/iris.data.txt"
path_source_cluster = "hdfs:/user/user87/projet-bd/data/iris/iris.data.txt"
path_dest_local = "file:////home/acuna/Projets/PROJET_BIG_DATA/Repository/output-local-test"
path_dest_cluster = "hdfs:/user/user87/projet-bd/output/iris/iris-basic-job"

if __name__ == "__main__":
    conf = SparkConf().setAppName('exercice')
    sc = SparkContext(conf=conf)

    lines = sc.textFile(path_source_cluster)

    data = lines.map(lambda x: x.split(','))\
            .map(lambda x: [float(i) for i in x[:4]]+[x[4]])\
            .zipWithIndex()\
            .map(lambda x: (x[1],x[0]))

    nb_clusters = 3

    nb_elem = sc.broadcast(data.count())

    centroides = sc.parallelize(data.takeSample('withoutReplacment', nb_clusters)).zipWithIndex().map(lambda x: (x[1], x[0][1][:-1]))

    joined = data.cartesian(centroides)

    dist = joined.map(lambda x: (x[0][0], (x[1][0], computeDistance(x[0][1][:-1], x[1][1]))))

    dist_list = dist.groupByKey().mapValues(list)

    min_dist = dist_list.mapValues(closestCluster)

    # assignment will be our return value : It contains the datapoint,
    # the id of the closest cluster and the distance of the point to the centroid

    assignment = min_dist.join(data)

    ############################################
    # Compute the new centroid of each cluster #
    ############################################

    clusters = assignment.map(lambda x: (x[1][0][0], x[1][1][:-1]))
    # (2, [5.1, 3.5, 1.4, 0.2])

    count = clusters.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
    somme = clusters.reduceByKey(sumList)
    centroidesCluster = somme.join(count).map(lambda x: (x[0], moyenneList(x[1][0], x[1][1])))

    error = sqrt(min_dist.map(lambda x: x[1][1]).reduce(lambda x, y: x + y)) / nb_elem.value

    centroides = centroidesCluster

    assignment.coalesce(1).saveAsTextFile(path_dest_cluster)


