#!/bin/bash

# HDFS operations (unable to run this as CloudXLab storage is full)
# Hdfs dfs -mkdir /user/bigdatacloudxlab27228/SRD_05312023_UK_Accidents_Curated

# Hive operations
hive_commands=$(cat <<'END_HIVE_COMMANDS'
show databases;
use srd_05212023_uk_accidents_hive_db;
show tables;
select * from uk_accidents limit 5;

CREATE EXTERNAL TABLE IF NOT EXISTS uk_accidents (
  Accident_Index STRING,
  Vehicle_Reference INT,
  Casualty_Reference INT,
  Casualty_Class INT,
  Sex_of_Casualty INT,
  Age_of_Casualty INT,
  Age_Band_of_Casualty INT,
  Casualty_Severity INT,
  Pedestrian_Location INT,
  Pedestrian_Movement INT,
  Car_Passenger INT,
  Bus_or_Coach_Passenger INT,
  Pedestrian_Road_Maintenance_Worker INT,
  Casualty_Type INT,
  Casualty_Home_Area_Type INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/bigdatacloudxlab27228/SRD_05312023_UK_Accidents_Landing';
END_HIVE_COMMANDS
)

echo "$hive_commands" | hive

# PySpark operations
pyspark_commands=$(cat <<'END_PYSPARK_COMMANDS'
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SHANTHAN").enableHiveSupport().getOrCreate()
spark.sql("USE srd_05212023_uk_accidents_hive_db")
spark.sql("show tables").show()
hive_df = spark.sql("select * from uk_accidents")
hive_df = spark.sql("SELECT * FROM uk_accidents WHERE Accident_Index != 'Accident_Index'")
hive_df.show()
replaced_df = hive_df.na.replace('', 'N/A')
replaced_df.write.mode("overwrite").parquet("/user/bigdatacloudxlab27228/SRD_05312023_UK_Accidents_Curated")

# Convert Parquet to CSV
csv_df = spark.read.parquet("/user/bigdatacloudxlab27228/SRD_05312023_UK_Accidents_Curated")
csv_df.write.mode("overwrite").csv("/user/bigdatacloudxlab27228/SRD_05312023_UK_Accidents_Curated_CSV")
END_PYSPARK_COMMANDS
)

echo "$pyspark_commands" | pyspark

# Sqoop export
sqoop export --connect jdbc:mysql://cxln2:3306/sqoopex --username sqoopuser --password NHkkP876rp --table SRD_06022023_UK_Accidents_SQOOP --export-dir hdfs://cxln1.c.thelab-240901.internal:8020/user/bigdatacloudxlab27228/SRD_05312023_UK_Accidents_Curated_CSV --input-fields-terminated-by ',' --input-lines-terminated-by '\n'
