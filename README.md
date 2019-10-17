
# Snowflake - Data Pipeline using Spark

### Scope of this project:
This exercise is to illustrate a simple Data pipeline using Apache Spark to Extract, Transform & Load data from/to Snowflake database.

### Pre-requisities
* Apache Spark installation (This project uses Spark 2.4, Scala 2.11.12)
* Snowflake Account
* AWS S3 storage (or any other cloud storage)


```python
import os
import configparser
from pyspark.sql import SparkSession
```

### Import AWS/Snowflake Credentials from Config File
* Create a .cfg file in your directory and store the credentials there.
* Template of credentials.cfg file is included in the repository under the name `credentials.template`



```python
config = configparser.ConfigParser()
config.read('credentials.cfg')
SF_URL = config['SNOWFLAKE']['URL']
SF_ACCOUNT = config['SNOWFLAKE']['ACCOUNT']
SF_USER = config['SNOWFLAKE']['USER']
SF_PASSWORD = config['SNOWFLAKE']['PASSWORD']
AWS_ACCESS_KEY_ID = config['AWS_CREDENTIALS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS_CREDENTIALS']['AWS_SECRET_ACCESS_KEY']
S3_BUCKET = config['AWS_S3']['S3_BUCKET']
#os.environ['AWS_ACCESS_KEY_ID']=AWS_ACCESS_KEY_ID
#os.environ['AWS_SECRET_ACCESS_KEY']=AWS_SECRET_ACCESS_KEY

```

### Initiate Spark Session

* Following packages/jars are needed to enable Snowflake connector and AWS connections. 
* They will be pulled from Maven Repository during Spark session initiation. https://repo1.maven.org/maven2/
* Please note the below Packages are for Spark version 2.4 and Scala version 2.11.12. If you are using different versions of Spark/Scala, then use the corresponding compatible JARs accordingly.

Package | Description
--------|--------------
org.apache.hadoop:hadoop-aws:2.7.3 | Hadoop jars for AWS S3 connection
net.snowflake:snowflake-jdbc:3.9.2 | Snowflake JDBC connector
net.snowflake:spark-snowflake_2.11:2.5.3-spark_2.4 | Snowflake connector for Spark 2.4


```python
spark = SparkSession \
    .builder \
    .appName("Rakesh_Test_Snowflake") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.3,net.snowflake:snowflake-jdbc:3.9.2,net.snowflake:spark-snowflake_2.11:2.5.3-spark_2.4") \
    .getOrCreate()
```

##### *Note: Snowflake USER account should have CREATE STAGE privileges on the schema/tables which are being queried. Because Snowflake connector stages the data during querying. Otherwise Spark throws error saying "Insufficient authorization privileges"*

### Set AWS credentials


```python
sc = spark.sparkContext
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
```

### Set Snowflake connection parameters
* URL, Account, Username, Password are stored in config file since they should not be exposed in code. Make sure not to push the config file in Public git repository


```python
sfOptions = {
  "sfURL" : SF_URL,
  "sfAccount" : SF_ACCOUNT,
  "sfUser" : SF_USER,
  "sfPassword" : SF_PASSWORD,
  "sfDatabase" : "DEMO_DB",
  "sfSchema" : "PUBLIC",
  "sfWarehouse" : "DEMO_WH",
  "sfRole" : "SYSADMIN",
}
```
..
## <mark>ETL - EXTRACT STEP</mark>

### Read data from Snowflake


```python
sql="""select s.S_SUPPKEY, s.S_NAME, n.N_NATIONKEY, n.N_NAME  
from supplier s 
left join nation n 
on s.s_nationkey = n.n_nationkey;
"""
 
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option('query', sql) \
    .load()
```

**Sample Input Data:**

S_SUPPKEY	|S_NAME	| N_NATIONKEY|	N_NAME
----------|-------|------------|-----------
0	343300	|Supplier#000343300 |	 6	| FRANCE
1	343301	|Supplier#000343301	| 14	| KENYA

  
..
## <mark>ETL - TRANSFORM STEP</mark>

### Perform Spark Transformation
  * Group data by Nation Name and get the counts for each Nation
  * Output data will have 2 columns - Nation Name, Supplier Nation Count


```python
df_stage1 = spark.sql("select n_name, count(*) as supplier_count from df_view group by 1")
```

### Write Transformed data into S3 bucket (To be consumed by Snowflake in next step)


```python
target_s3_path = f'{S3_BUCKET}/supplier_nation_counts'
df_stage1.write.mode('overwrite').csv(target_s3_path, header=True)
```

### Data Validation
* Validate whether the data is loaded into S3 properly by reading the loaded S3 data and get the count. Count should not be 0.


```python
spark.read.csv(target_s3_path, header=True).count()
```
..

## <mark>ETL - LOAD STEP</mark>

### Copy data from S3 output bucket into Snowflake Target table

**Method 1**. Write Spark Dataframe directly into Snowflake Target Table (Target Table name :  supplier_nation_counts)


```python
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
df_stage1.write.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option("dbtable", "supplier_nation_counts") \
    .mode('overwrite') \
    .save()
```

**Method 2**. COPY command (Bulk Copy) - Can be run using script or in Snowflake directly

```copy into supplier_nation_counts
from target_s3_path credentials=(aws_key_id=AWS_ACCESS_KEY_ID aws_secret_key='AWS_SECRET_ACCESS_KEY')
file_format = (type = csv field_delimiter = ',' skip_header = 1);
```
.
### Finally!!! -- Read data from Snowflake Target table to verify whether the data is loaded properly


```python
sql="select * from supplier_nation_counts"
 
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
df2 = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
    .options(**sfOptions) \
    .option('query', sql) \
    .load()
df2.show(2)
```

**Sample Output Data:**

N_NAME|SUPPLIER_COUNT
------|--------------
FRANCE|         40126
 JAPAN|         39862


    only showing top 2 rows
