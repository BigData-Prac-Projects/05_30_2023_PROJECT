hive -e "
USE sv_train_landing;
CREATE EXTERNAL TABLE if not exists train (
    key string,
    fare_amount double,
    pickup_datetime string,
    pickup_longitude string,
    pickup_latitude string,
    dropoff_longitude double,
    dropoff_latitude double,
    passenger_count int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '\"'
)
LOCATION '/user/bigdatacloudxlab27228/hdfs_train_landing'
TBLPROPERTIES ('skip.header.line.count'='1');
select count(*) from train;"
# Read data in Spark and write to Parquet files
spark=$(cat <<'END_SPARK'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Dataframe').enableHiveSupport().getOrCreate()
spark.sql("use sv_train_landing")
df = spark.sql("select * from train")
df.printSchema()
df.show()
df2 = df.fillna('N/A')
df2.write.parquet("/user/bigdatacloudxlab27228/hdfs_train_curated")
df2.write.mode("overwrite").csv("/user/bigdatacloudxlab27228/hdfs_train_curated.csv")
END_SPARK
)
# Sqoop export
sqoop export \
  --connect jdbc:mysql://cxln2:3306/sqoopex \
  --username sqoopuser \
  --password NHkkP876rp \
  --table train \
  --export-dir hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab27228/hdfs_train_curated.csv \
  --input-fields-terminated-by ',' \
  --input-lines-terminated-by '\n'