import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
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

# Script generated for node Amazon Kinesis
dataframe_AmazonKinesis_node1747228674663 = glueContext.create_data_frame.from_options(connection_type="kinesis",connection_options={"typeOfData": "kinesis", "streamARN": "arn:aws:kinesis:eu-west-1:050451375647:stream/telco-stream", "classification": "json", "startingPosition": "earliest", "inferSchema": "true"}, transformation_ctx="dataframe_AmazonKinesis_node1747228674663")

def processBatch(data_frame, batchId):
    if (data_frame.count() > 0):
        AmazonKinesis_node1747228674663 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        # Script generated for node VALIDATE RECORDS
        SqlQuery111 = '''
        SELECT *
        FROM myDataSource
        WHERE hour IS NOT NULL
          AND lat IS NOT NULL
          AND long IS NOT NULL
          AND signal IS NOT NULL
          AND network IS NOT NULL
          AND operator IS NOT NULL
          AND status IS NOT NULL
          AND description IS NOT NULL
          AND speed IS NOT NULL
          AND satellites IS NOT NULL
          AND precission IS NOT NULL
          AND provider IS NOT NULL
          AND activity IS NOT NULL
          AND postal_code IS NOT NULL;
        '''
        VALIDATERECORDS_node1747230285338 = sparkSqlQuery(glueContext, query = SqlQuery111, mapping = {"myDataSource":AmazonKinesis_node1747228674663}, transformation_ctx = "VALIDATERECORDS_node1747230285338")

        # Script generated for node Average Precision and GPS Precision
        SqlQuery112 = '''
        SELECT 
          operator,
          ROUND(AVG(CAST(signal AS DOUBLE)), 1) AS avg_signal_strength,
          ROUND(AVG(CAST(precission AS DOUBLE)), 1) AS avg_gps_precision
        FROM myDataSource
        GROUP BY operator;
        '''
        AveragePrecisionandGPSPrecision_node1747231077748 = sparkSqlQuery(glueContext, query = SqlQuery112, mapping = {"myDataSource":VALIDATERECORDS_node1747230285338}, transformation_ctx = "AveragePrecisionandGPSPrecision_node1747231077748")

        # Script generated for node Network Status
        SqlQuery113 = '''
        SELECT 
          postal_code,
          COUNT(*) AS status_count
        FROM myDataSource
        GROUP BY postal_code;
        '''
        NetworkStatus_node1747231563818 = sparkSqlQuery(glueContext, query = SqlQuery113, mapping = {"myDataSource":VALIDATERECORDS_node1747230285338}, transformation_ctx = "NetworkStatus_node1747231563818")

        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        hour = now.hour

        # Script generated for node Bronze
        Bronze_node1747228850253_path = "s3://mobile-logs21/bronze" + "/ingest_year=" + "{:0>4}".format(str(year)) + "/ingest_month=" + "{:0>2}".format(str(month)) + "/ingest_day=" + "{:0>2}".format(str(day)) + "/ingest_hour=" + "{:0>2}".format(str(hour))  + "/"
        Bronze_node1747228850253 = glueContext.write_dynamic_frame.from_options(frame=AmazonKinesis_node1747228674663, connection_type="s3", format="glueparquet", connection_options={"path": Bronze_node1747228850253_path, "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Bronze_node1747228850253")

        # Script generated for node SILVER 
        additional_options = {"path": "s3://mobile-logs21/silver/", "write.parquet.compression-codec": "snappy"}
        SILVER_node1747230786457_df = VALIDATERECORDS_node1747230285338.toDF()
        SILVER_node1747230786457_df.write.format("delta").options(**additional_options).partitionBy("operator").mode("append").save()

        # Script generated for node Gold Storage
        additional_options = {"path": "s3://mobile-logs21/gold/avg_Precision/", "write.parquet.compression-codec": "snappy"}
        GoldStorage_node1747231491605_df = AveragePrecisionandGPSPrecision_node1747231077748.toDF()
        GoldStorage_node1747231491605_df.write.format("delta").options(**additional_options).mode("append").save()

        # Script generated for node Gold Status Count
        additional_options = {"path": "s3://mobile-logs21/gold/status_count/", "write.parquet.compression-codec": "snappy"}
        GoldStatusCount_node1747231699744_df = NetworkStatus_node1747231563818.toDF()
        GoldStatusCount_node1747231699744_df.write.format("delta").options(**additional_options).mode("append").save()

glueContext.forEachBatch(frame = dataframe_AmazonKinesis_node1747228674663, batch_function = processBatch, options = {"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"})
job.commit()