import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from sodasql.soda_server_client.soda_server_client import SodaServerClient
from sodaspark import scan
import os



# Your S3 Credentials goes here:
s3AccessKeyId=''
s3SecretAccessKey=''
# Your Soda Cloud credentials goes here:
sodaCloudAPIKey=''
sodaCloudAPISecret=''


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0 pyspark-shell'

#spark configuration
conf = SparkConf().setAppName('soda_spark_s3').setMaster('local[*]')
conf.set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
conf.set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
#This would have to be updated when using other Authentication options. It defaults to IAM.
conf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')


sc=SparkContext(conf=conf)
sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', s3AccessKeyId)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', s3SecretAccessKey)
sc._jsc.hadoopConfiguration().set('fs.s3a.path.style.access', 'true')
sc._jsc.hadoopConfiguration().set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')


spark=SparkSession(sc)

#This is where you instruct spark to look for a csv/json and the formats:
s3_df=spark.read.json('s3a://soda-spark-orders-json/')

s3_df.printSchema()


soda_server_client = SodaServerClient(
    host="cloud.soda.io",
    api_key_id=sodaCloudAPIKey,
    api_key_secret=sodaCloudAPISecret)

#Perform scan
scan_definition = 'orders.yml'
scan_result = scan.execute(scan_definition, s3_df, soda_server_client=soda_server_client)
print(scan_result.measurements)
