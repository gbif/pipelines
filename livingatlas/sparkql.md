#Spark QL Helper

These are some examples for how to use Spark QL on local development machines or on the cluster.


## Load a dataframe from avro

Start spark shell with the following command. `spark-shell` 

`
$SPARK_HOME/bin/spark-shell --packages org.apache.spark:spark-avro_2.11:2.4.4
`

Load avro files

`
scala> val verbatimDF = spark.read.format("avro").load("/data/pipelines-data/dr2009/1/verbatim.avro")
verbatimDF: org.apache.spark.sql.DataFrame = [id: string, coreRowType: string ... 2 more fields]
`

# Record count

`
scala> verbatimDF.count
res1: Long = 18675270 
`

# Spark QL

spark.sqlContext.sql("CREATE TEMPORARY VIEW VERBATIM USING avro OPTIONS (path \"/data/pipelines-data/dr2009/1/verbatim.avro\")")

# Retrieve a single record

spark.sqlContext.sql("SELECT coreTerms FROM VERBATIM where id='944d2aea-58ee-4e53-aecf-19c46b061d09'").show(false)


