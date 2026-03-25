CREATE EXTERNAL TABLE step_trainer_landing (
    sensorreadingtime bigint,
    serialnumber string,
    distancefromobject int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aws-glue-assets-495040113705-us-east-1/step_trainer_landing/';

#Validation query
SELECT COUNT(*) FROM step_trainer_landing;
