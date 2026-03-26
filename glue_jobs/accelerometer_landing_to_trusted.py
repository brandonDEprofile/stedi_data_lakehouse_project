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
AWSGlueDataCatalog_node1774473316891 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1774473316891")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1774474495255 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1774474495255")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT a.*
FROM accelerometer_landing a
JOIN customer_trusted c
ON a.user = c.email
'''
SQLQuery_node1774474702488 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":AWSGlueDataCatalog_node1774473316891, "customer_trusted":AWSGlueDataCatalog_node1774474495255}, transformation_ctx = "SQLQuery_node1774474702488")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1774474702488, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1774474478906", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1774474856252 = glueContext.getSink(path="s3://aws-glue-assets-495040113705-us-east-1/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1774474856252")
AmazonS3_node1774474856252.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1774474856252.setFormat("json")
AmazonS3_node1774474856252.writeFrame(SQLQuery_node1774474702488)
job.commit()
