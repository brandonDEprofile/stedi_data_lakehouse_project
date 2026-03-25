import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1774477168395 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1774477168395")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1774477241119 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1774477241119")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT
    s.sensorReadingTime AS sensorreadingtime,
    s.serialNumber AS serialnumber,
    s.distanceFromObject AS distancefromobject,
    a.timestamp AS timestamp,
    a.x AS x,
    a.y AS y,
    a.z AS z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a
ON s.sensorReadingTime = a.timestamp
'''
SQLQuery_node1774477267592 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":AWSGlueDataCatalog_node1774477241119, "accelerometer_trusted":AWSGlueDataCatalog_node1774477168395}, transformation_ctx = "SQLQuery_node1774477267592")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1774477267592, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1774474478906", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1774477370001 = glueContext.getSink(path="s3://aws-glue-assets-495040113705-us-east-1/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1774477370001")
AmazonS3_node1774477370001.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1774477370001.setFormat("json")
AmazonS3_node1774477370001.writeFrame(SQLQuery_node1774477267592)
job.commit()
