package org.gbif.pipelines.ingest.pipelines

import org.apache.spark.sql.SparkSession


class OccurrenceToEsIndexPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Gridded datasets")
      .getOrCreate()

  }

}
