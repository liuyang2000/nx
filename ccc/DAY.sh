spark-submit --master yarn --conf "spark.memory.storageFraction=0.15" --driver-memory 2G --executor-memory 2G --executor-cores 4 --num-executors 2 --class spark.task.NX.submitTask nx-1.0-jar-all.jar GDTJ oozie DAY

spark-submit --master yarn --conf "spark.memory.storageFraction=0.15" --driver-memory 2G --executor-memory 2G --executor-cores 4 --num-executors 2 --class spark.task.NX.submitTask nx-1.0-jar-all.jar TYDB

spark-submit --master yarn --conf "spark.memory.storageFraction=0.15" --conf "spark.eventLog.enabled=false" --driver-memory 2G --executor-memory 2G --executor-cores 4 --num-executors 2 --class spark.task.PSR.submitTask nx-1.0-jar-all.jar ATMNL oozie


