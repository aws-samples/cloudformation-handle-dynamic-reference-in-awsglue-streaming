import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from botocore.exceptions import ClientError

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME","tgt_s3_bkt"])
sc = SparkContext()
glueContext = GlueContext(SparkContext.getOrCreate("default"))
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

def monitorChangeFlag():
    change_flag=False # Default to false
    try:
        client = boto3.client("s3")
        s3 = boto3.resource('s3')
        obj = client.head_object(Bucket=bucket_nm, Key=change_key)
        change_flag=True
        s3.Object(bucket_nm, change_key).delete() # Delete the change flag to avoid redundant refreshes
    except ClientError as exc:
        if exc.response['Error']['Code'] != '404':
            change_flag=False
    return change_flag

def refreshReferenceData():
    dyf_dynamodb_reference_df = glueContext.create_dynamic_frame.from_catalog(
        database="my-database",
        table_name="productpriority"
        )
    df_dynamodb_reference = dyf_dynamodb_reference_df.toDF()
    return df_dynamodb_reference

# The S3 bucket name where the target data is stored
bucket_nm=args["tgt_s3_bkt"]
change_key = 'change_flags/CHANGE_FLAG'

# Read Glue Catalog Table built on the Kinesis Data Stream
df_SourceKinesisStream = glueContext.create_data_frame.from_catalog(
    database="my-database",
    table_name="source-kinesis-stream-tbl",
    additional_options={"startingPosition": "latest", "inferSchema": "false"},
    transformation_ctx="df_SourceKinesisStream",
)

df_dynamodb_reference = refreshReferenceData()

def processBatch(data_frame, batchId):
    # Declare global variables. Required since the reference data is initialized conditionally within the batch process method
    global df_dynamodb_reference

    logger.info("Starting microbatch...")
    hasChanged = monitorChangeFlag()
    
    # If there are changes or if this is the first run, refresh the reference data
    if hasChanged or df_dynamodb_reference is None:
        df_dynamodb_reference = refreshReferenceData()
    
    # Perform the join using Spark inner join
    df_join = data_frame.join(df_dynamodb_reference, data_frame.dish==df_dynamodb_reference.item,"inner")
    
    # Convert output of join to a Glue Dynamic Frame
    dyf_join = DynamicFrame.fromDF(df_join, glueContext, "from_data_frame1")
    
    # Select only required fields
    dyf_SelectFields = SelectFields.apply(
        frame=dyf_join,
        paths=["cost", "price", "priority", "customer_id", "item"],
        transformation_ctx="dyf_SelectFields",
    )
                
    # Path where the target data should be stored
    s3_path = f"s3://{bucket_nm}/demo-final-data/"
    
    # Define the sink object
    s3_sink = glueContext.getSink(
        path=s3_path,
        connection_type="s3",
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["item", "priority"],
        enableUpdateCatalog=True,
        transformation_ctx="s3_sink",
    )
    logger.info("Sink Established")
    
    s3_sink.setCatalogInfo(
        catalogDatabase="my-database", catalogTableName="demo-final-data"
    )
    logger.info("Catalog Established")
    
    s3_sink.setFormat("csv")
    logger.info("Format Established")
    
    s3_sink.writeFrame(dyf_SelectFields)
    logger.info("Written Batch to S3")


# Process Microbatches of real time data
glueContext.forEachBatch(
    frame=df_SourceKinesisStream,
    batch_function=processBatch,
    options={
        "windowSize": "10 seconds",
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)
job.commit()
