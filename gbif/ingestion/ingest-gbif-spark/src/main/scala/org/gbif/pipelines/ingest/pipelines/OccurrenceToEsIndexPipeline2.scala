package org.gbif.pipelines.ingest.pipelines

import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}


object OccurrenceToEsIndexPipeline2 {

  def recordToString(row: Row): String = {
    s"ID: ${row.getAs[String]("id")}, Identifier: ${row.getAs[String]("ir")}, Basic: ${row.getAs[String]("br")}}"
  }

  val constructPathFn: (String, String) => String = (path, name) =>
    s"$path/occurrence/$name/*.avro"

  val readAvroFn: (SparkSession, String, String) => DataFrame = (session, path, name) =>
    session
      .read
      .format("avro")
      .load(constructPathFn.apply(path, name))

  def main(args: Array[String]): Unit = {
    run(args)
  }

  def run(args: Array[String]): Unit = {

    val path = args(0)

    val spark = SparkSession
      .builder()
      .appName(s"indexing_$path")
      .getOrCreate()

    val idDf =
      readAvroFn.apply(spark, path, "identifier")
        .alias("ir")

    val basicDf =
      readAvroFn.apply(spark, path, "basic")
        .alias("br")

    val taxonomyDf =
      readAvroFn.apply(spark, path, "taxonomy")
        .alias("tr")

    val mergedDf = idDf
      .join(basicDf, "id")
      .join(taxonomyDf, "id")

    mergedDf.printSchema()

    mergedDf.show(10)

    val mergedRecords = mergedDf.map(row => recordToString(row))(Encoders.STRING)

    mergedRecords.show(10)
  }

}
