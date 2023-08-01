sqoop export \
  --connect jdbc:mysql://cxln2:3306/sqoopex \
  --username sqoopuser \
  --password NHkkP876rp \
  --table train \
  --export-dir hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab27228/hdfs_train_curated.csv \
  --input-fields-terminated-by ',' \
  --input-lines-terminated-by '\n'
