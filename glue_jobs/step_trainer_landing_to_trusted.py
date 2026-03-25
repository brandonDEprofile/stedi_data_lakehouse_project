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
AWSGlueDataCatalog_node1774478635273 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="AWSGlueDataCatalog_node1774478635273")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1774478628095 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="AWSGlueDataCatalog_node1774478628095")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT s.*
FROM step_trainer_landing s
JOIN customers_curated c
ON s.serialnumber = c.serialnumber
'''
SQLQuery_node1774476708971 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":AWSGlueDataCatalog_node1774478628095, "customers_curated":AWSGlueDataCatalog_node1774478635273}, transformation_ctx = "SQLQuery_node1774476708971")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1774476708971, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1774474478906", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1774476795849 = glueContext.getSink(path="s3://aws-glue-assets-495040113705-us-east-1/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1774476795849")
AmazonS3_node1774476795849.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1774476795849.setFormat("json")
AmazonS3_node1774476795849.writeFrame(SQLQuery_node1774476708971)
job.commit()
