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
