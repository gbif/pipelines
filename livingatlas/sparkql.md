# Spark QL Helper

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

## Record count

`
scala> verbatimDF.count
res1: Long = 18675270 
`

## Spark QL
```
spark.sqlContext.sql("CREATE TEMPORARY VIEW VERBATIM USING avro OPTIONS (path \"/data/pipelines-data/dr2009/1/verbatim.avro\")")
```

```
spark.sqlContext.sql("CREATE TEMPORARY VIEW VERBATIM_DR2009 USING avro OPTIONS (path \"hdfs://aws-spark-quoll-master.ala:9000/pipelines-data/dr2009/1/verbatim.avro\")")
spark.sqlContext.sql("CREATE TEMPORARY VIEW UUID_DR2009 USING avro OPTIONS (path \"hdfs://aws-spark-quoll-master.ala:9000/pipelines-data/dr2009/1/identifiers/ala_uuid/*.avro\")")
spark.sqlContext.sql("CREATE TEMPORARY VIEW UUID_DR359 USING avro OPTIONS (path \"hdfs://aws-spark-quoll-master.ala:9000/pipelines-data/dr359/1/identifiers/ala_uuid/*.avro\")")
spark.sqlContext.sql("CREATE TEMPORARY VIEW VERBATIM USING avro OPTIONS (path \"hdfs://aws-spark-quoll-master.ala:9000/pipelines-data/*/1/verbatim.avro\")")

spark.sqlContext.sql("SELECT * FROM UUID_DR2009").show(false)
spark.sqlContext.sql("SELECT * FROM UUID_DR359").show(false)
spark.sqlContext.sql("SELECT * FROM VERBATIM_DR2009").show(false)
```


## Retrieve a single record

```
spark.sqlContext.sql("SELECT coreTerms FROM VERBATIM where id='944d2aea-58ee-4e53-aecf-19c46b061d09'").show(false)

spark.sqlContext.sql("SELECT id, coreTerms FROM VERBATIM where id='944d2aea-58ee-4e53-aecf-19c46b061d09'").show(false)

spark.sqlContext.sql("SELECT id, coreTerms FROM VERBATIM").show(false)
```

