CREATE EXTERNAL TABLE customer_landing (
    serialnumber string,
    sharewithpublicasofdate bigint,
    birthday string,
    registrationdate bigint,
    sharewithresearchasofdate bigint,
    customername string,
    email string,
    lastupdatedate bigint,
    phone string,
    sharewithfriendsasofdate bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://aws-glue-assets-495040113705-us-east-1/customer_landing/';



#Validation query
SELECT COUNT(*) FROM customer_landing;
