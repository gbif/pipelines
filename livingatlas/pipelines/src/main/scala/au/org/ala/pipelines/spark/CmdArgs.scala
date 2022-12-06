package au.org.ala.pipelines.spark
import com.beust.jcommander.{JCommander, Parameter, Parameters}

import java.util.Collections

/** Command line parameter parsing for running the pipeline
  */
@Parameters(separators = "=")
class CmdArgs {

  @Parameter(
    names = Array("--datasetId"),
    description = "The dataset ID e.g. dr123",
    required = true
  )
  var datasetId: String = null

  @Parameter(
    names = Array("--query"),
    description = "Predicate JSON query ",
    required = false
  )
  var query: String = null

  @Parameter(
    names = Array("--queryFile"),
    description = "Absolute path to a file containing predicate JSON query ",
    required = false
  )
  var queryFilePath: String = null

  @Parameter(
    names = Array("--inputPath"),
    description = "Filesystem / HDFS path to input AVRO",
    required = false
  )
  var inputPath: String = null

  @Parameter(
    names = Array("--targetPath"),
    description = "Filesystem / HDFS path to input AVRO",
    required = false
  )
  var targetPath: String = null

  @Parameter(
    names = Array("--localExportPath"),
    description = "Filesystem / HDFS path to DwCA output ",
    required = true
  )
  var localExportPath: String = null

  @Parameter(
    names = Array("--attempt"),
    description = "Attempt number",
    required = false
  )
  var attempt: Int = 1

  @Parameter(
    names = Array("--hdfsSiteConfig"),
    description = "Path to HDFS config",
    required = false
  )
  var hdfsSiteConfig: String = null

  @Parameter(
    names = Array("--coreSiteConfig"),
    description = "Path to Core site HDFS config",
    required = false
  )
  var coreSiteConfig: String = null

  @Parameter(
    names = Array("--properties"),
    description = "Properties YAML file",
    required = false
  )
  var properties: String = null

  @Parameter(names = Array("--jobId"), description = "Job ID", required = false)
  var jobId: String = null

  @Parameter(
    names = Array("--skipEventExportFields"),
    description = "List of fields to ignore",
    required = false,
    splitter = classOf[YamlListSplitter]
  )
  var skipEventExportFields: java.util.List[String] = Collections.emptyList()

  @Parameter(
    names = Array("--skipOccurrenceExportFields"),
    description = "List of fields to ignore",
    required = false,
    splitter = classOf[YamlListSplitter]
  )
  var skipOccurrenceExportFields: java.util.List[String] = Collections.emptyList()

  @Parameter(names = Array("--experiments"), description = "Ignore", required = false)
  var experiments: String = null
}

import com.beust.jcommander.{Parameter, Parameters}
import com.beust.jcommander.converters.IParameterSplitter

import java.util
import java.util.Collections

class YamlListSplitter extends IParameterSplitter {
  override def split(value: String): java.util.List[String] = {
    val cleaned = value.substring(1, value.length() - 1)
    val values = cleaned.split(",").map(_.trim)
    util.Arrays.asList[String](values: _*)
  }
}
