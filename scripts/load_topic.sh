topic=$1

hadoop jar kafka-hadoop-loader.jar -z sa-zk-001.high.caffeine.io:2181 -t $topic -c off /data/analytics/json/$topic
