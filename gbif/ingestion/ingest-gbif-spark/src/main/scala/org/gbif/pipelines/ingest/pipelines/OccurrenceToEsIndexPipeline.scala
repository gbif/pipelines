package org.gbif.pipelines.ingest.pipelines

import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.gbif.pipelines.ingest.config.Conf
import org.gbif.pipelines.io.avro.{BasicRecord, IdentifierRecord, TaxonRecord}


object OccurrenceToEsIndexPipeline {

  def recordToString(id: String, ir: IdentifierRecord, br: BasicRecord, tr: TaxonRecord): String = {
    s"ID: ${id}, Identifier: ${ir.toString}, Basic: ${br.toString}, Taxonomy: ${tr.toString}"
  }

  val constructPathFn: (Conf, String) => String = (conf, name) =>
    s"${conf.inputAvroPath}/${conf.datasetKey}/${conf.attempt}/occurrence/$name/*.avro"

  val readAvroFn: (SparkSession, Conf, String) => DataFrame = (session, conf, name) =>
    session
      .read
      .format("avro")
      .load(constructPathFn.apply(conf, name))

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    run(conf)
  }

  def run(conf: Conf): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"indexing_${conf.datasetKey}_${conf.attempt}")
      .getOrCreate()

    val idDf =
      readAvroFn.apply(spark, conf, "identifier")
        .as(Encoders.bean(classOf[IdentifierRecord]))
        .alias("ir")

    val basicDf =
      readAvroFn.apply(spark, conf, "basic")
        .as(Encoders.bean(classOf[BasicRecord]))
        .alias("br")

    val taxonomyDf =
      readAvroFn.apply(spark, conf, "taxonomy")
        .as(Encoders.bean(classOf[TaxonRecord]))
        .alias("tr")

    val mergedDf = idDf
      .join(basicDf, "id")
      .join(taxonomyDf, "id")

    val mergedRecords = mergedDf.map(row => recordToString(
      row.getAs[String]("id"),
      row.getAs[IdentifierRecord]("ir"),
      row.getAs[BasicRecord]("br"),
      row.getAs[TaxonRecord]("tr")
    ))(Encoders.STRING)

    mergedRecords.show(10)
  }

}
