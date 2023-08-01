mkdir /home/bigdatacloudxlab27228/sv
hdfs dfs -mkdir sv
hdfs dfs -copyFromLocal /home/bigdatacloudxlab27228/sv/Listings.csv /user/bigdatacloudxlab27228/sv/
hive -f /home/bigdatacloudxlab27228/sv/hive.hql
spark-submit /home/bigdatacloudxlab27228/sv/spark.py
sh /home/bigdatacloudxlab27228/sv/sqoop.sh
