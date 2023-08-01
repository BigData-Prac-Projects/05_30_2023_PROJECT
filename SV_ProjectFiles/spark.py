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
