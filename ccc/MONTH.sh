spark-submit --master yarn --conf "spark.memory.storageFraction=0.15" --driver-memory 2G --executor-memory 2G --executor-cores 4 --num-executors 2 --class spark.task.NX.submitTask nx-1.0-jar-all.jar GDTJ oozie MONTH
