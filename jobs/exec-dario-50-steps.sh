start6=$(date +%s.%N)
spark-submit \
--master yarn --deploy-mode cluster \
--executor-cores 4 \
--num-executors 11 \
--executor-memory 5g \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.driver.memory=5g \
--conf spark.driver.cores=1 \
--conf spark.yarn.jars="file:///home/cluster/shared/spark/jars/*.jar" \
$HOME_CLUST/python/kmeans-dario-50-steps.py
end6=$(date +%s.%N)

runtime6=$((end6 - start6))
runtime6>time_10_steps.txt
