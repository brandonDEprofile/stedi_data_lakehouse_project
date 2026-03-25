CREATE EXTERNAL TABLE accelerometer_landing (
    timestamp bigint,
    user string,
    x double,
    y double,
    z double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aws-glue-assets-495040113705-us-east-1/accelerometer_landing/';


#Validation query
SELECT COUNT(*) FROM accelerometer_landing;
