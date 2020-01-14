start2=$(date +%s
spark-submit \
--master yarn --deploy-mode cluster \
--executor-cores 4 \
--num-executors 11 \
--executor-memory 5g \
--conf spark.yarn.executor.memoryOverhead=2g \
--conf spark.driver.memory=5g \
--conf spark.driver.cores=1 \
--conf spark.yarn.jars="file:///home/cluster/shared/spark/jars/*.jar" \
$HOME_CLUST/python/kmeans-dario-15-steps.py
end2=$(date +%s)

python -c "print('temps exec : ' + str(${end2} - ${start2}) + 's')" >./logs/time_15_steps.txt
