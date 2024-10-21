package org.gbif.pipelines.ingest.config

import org.rogach.scallop._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val inputAvroPath = opt[String](required = true, descr = "Full path to AVRO files")
  val datasetKey = opt[String](required = true, descr = "Registry dataset key")
  val attempt = opt[Int](required = true, descr = "Registry dataset crawl attempt")

  verify()
}
