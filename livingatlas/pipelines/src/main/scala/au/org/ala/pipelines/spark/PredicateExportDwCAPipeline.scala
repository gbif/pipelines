package au.org.ala.pipelines.spark

import au.org.ala.kvs.{ALAPipelinesConfig, ALAPipelinesConfigFactory}

import _root_.java.io._
import _root_.java.net._
import _root_.java.util.{Collections, UUID}
import _root_.java.util.zip._
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, concat_ws}
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  DecimalType,
  DoubleType,
  IntegerType,
  LongType,
  StringType,
  StructType
}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.gbif.api.model.common.search.SearchParameter
import org.gbif.api.model.predicate.Predicate
import org.gbif.dwc.terms.DwcTerm
import au.org.ala.predicate.{ALAEventSearchParameter, ALAEventSparkQueryVisitor, ALAEventTermsMapper}
import au.org.ala.utils.CombinedYamlConfiguration
import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.gbif.pipelines.core.pojo.HdfsConfigs
import org.slf4j.LoggerFactory

import java.nio.channels.Channels
import java.nio.file.Files
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, Node, PrettyPrinter}

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
    names = Array("--skipExportFields"),
    description = "List of fields to ignore",
    required = false,
    splitter = classOf[YamlListSplitter]
  )
  var skipExportFields: java.util.List[String] = Collections.emptyList()

  @Parameter(names = Array("--experiments"), description = "Ignore", required = false)
  var experiments: String = null
}

import com.beust.jcommander.converters.IParameterSplitter

class YamlListSplitter extends IParameterSplitter {
  override def split(value: String): java.util.List[String] = {
    val cleaned = value.substring(1, value.length() - 1)
    val values = cleaned.split(",").map(_.trim)
    util.Arrays.asList[String](values: _*)
  }
}

/** Pipeline uses Spark SQL to produce a DwCA Archive.
  */
object PredicateExportDwCAPipeline {

  val log = LoggerFactory.getLogger(this.getClass)

  // Test with some sample data
  def main(args: Array[String]): Unit = {

    val combinedArgs = new CombinedYamlConfiguration(args: _*)
      .toArgs("general", "predicate-export")
    val exportArgs = new CmdArgs
    val cmd = JCommander.newBuilder.addObject(exportArgs).build

    try {
      cmd.parse(combinedArgs: _*)
    } catch {
      case e: Exception => {
        log.error(e.getMessage)
        cmd.usage()
        return
      }
    }

    val jobID = if (exportArgs.jobId != null && !exportArgs.jobId.isEmpty) {
      exportArgs.jobId
    } else {
      UUID.randomUUID.toString
    }

    // Process the query filter
    val queryFilter = if (exportArgs.queryFilePath != null && !exportArgs.queryFilePath.isEmpty) {
      val lines = scala.io.Source.fromFile(exportArgs.queryFilePath).mkString
      val om = new ObjectMapper()
      om.addMixIn(classOf[SearchParameter], classOf[ALAEventSearchParameter])
      val predicate = om.readValue(lines, classOf[Predicate])
      val v = new ALAEventSparkQueryVisitor(new ALAEventTermsMapper())
      v.buildQuery(predicate)
    } else if (exportArgs.query != null) {
      val om = new ObjectMapper()
      val unescaped = exportArgs.query.replaceAll("\\\\", "")
      om.addMixIn(classOf[SearchParameter], classOf[ALAEventSearchParameter])
      val predicate = om.readValue(unescaped, classOf[Predicate])
      val v = new ALAEventSparkQueryVisitor(new ALAEventTermsMapper())
      v.buildQuery(predicate)
    } else {
      ""
    }

    val hdfsConfigs = HdfsConfigs.create(exportArgs.hdfsSiteConfig, exportArgs.coreSiteConfig)
    val config: ALAPipelinesConfig = ALAPipelinesConfigFactory.getInstance(hdfsConfigs, exportArgs.properties).get

    log.info(s"Export for ${exportArgs.datasetId} - jobId ${jobID}")
    log.info(s"Generated query: $queryFilter")

    val exportPath = if (exportArgs.jobId != null && !exportArgs.jobId.isEmpty) {

      exportArgs.localExportPath + "/" + exportArgs.jobId
    } else {
      exportArgs.localExportPath
    }

    runExport(
      exportArgs.datasetId,
      exportArgs.inputPath,
      exportPath,
      exportArgs.attempt,
      queryFilter,
      exportArgs.skipExportFields.toArray(Array[String]()),
      exportArgs.hdfsSiteConfig,
      exportArgs.coreSiteConfig,
      config.collectory.getWsUrl()
    )
  }

  def runExport(
      datasetId: String,
      inputPath: String,
      localExportPath: String,
      attempt: Int,
      queryFilter: String,
      skippedFields: Array[String],
      hdfsSiteConf: String,
      coreSiteConf: String,
      registryUrl: String
  ) = {

    val outputDir = new File(localExportPath + "/" + datasetId)
    if (outputDir.exists()) {
      FileUtils.forceDelete(outputDir)
    }

    // Mask log
    val spark = SparkSession.builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

    log.info("Load search index")
    val eventSearchDF = spark.read
      .format("avro")
      .load(s"${inputPath}/${datasetId}/${attempt}/search/event/*.avro")
      .as("Search")

    // get a list columns
    val exportPath = s"$localExportPath/$datasetId/Event/"

    val filterSearchDF = if (queryFilter != "") {
      // filter "coreTerms", "extensions"
      eventSearchDF.filter(queryFilter).select(col("Search.id"))
    } else {
      eventSearchDF.select(col("Search.id"))
    }

    log.info("Load event core")
    val eventCoreDF = spark.read
      .format("avro")
      .load(s"${inputPath}/${datasetId}/${attempt}/event/event/*.avro")
      .as("Core")

    log.info("Load location")
    val locationDF = spark.read
      .format("avro")
      .load(s"${inputPath}/${datasetId}/${attempt}/event/location/*.avro")
      .as("Location")

    log.info("Load temporal")
    val temporalDF = spark.read
      .format("avro")
      .load(s"${inputPath}/${datasetId}/${attempt}/event/temporal/*.avro")
      .as("Temporal")

    log.info("Join")
    val filterDownloadDF = filterSearchDF
      .select(col("Search.id"))
      .join(eventCoreDF, col("Search.id") === col("Core.id"), "inner")
      .join(locationDF, col("Search.id") === col("Location.id"), "inner")
      .join(temporalDF, col("Search.id") === col("Temporal.id"), "inner")

    // generate interpreted event export
    log.info("Export interpreted event data")
    val (eventExportDF, eventFields) =
      generateInterpretedExportDF(filterDownloadDF, skippedFields, Array("Core", "Location", "Temporal"))
    eventExportDF.write
      .option("header", "true")
      .option("sep", "\t")
      .mode("overwrite")
      .csv(exportPath)

    cleanupFileExport("Event", hdfsSiteConf, coreSiteConf, localExportPath + s"/$datasetId")

    // load the verbatim DF
    val verbatimDF = spark.read
      .format("avro")
      .load(s"${inputPath}/${datasetId}/${attempt}/verbatim/*.avro")
      .as("Verbatim")

    // export the supplied core verbatim
    val verbatimCoreFields = exportVerbatimCore(
      spark,
      verbatimDF,
      filterDownloadDF,
      hdfsSiteConf,
      coreSiteConf,
      localExportPath + s"/$datasetId"
    )

    // export the supplied extensions verbatim
    val verbatimExtensionsForMeta = exportVerbatimExtensions(
      spark,
      verbatimDF,
      filterDownloadDF,
      hdfsSiteConf,
      coreSiteConf,
      localExportPath + s"/$datasetId"
    )

    // export interpreted occurrence
    val occurrenceFields = exportInterpretedOccurrence(
      datasetId,
      inputPath,
      attempt,
      spark,
      filterDownloadDF,
      verbatimExtensionsForMeta.keySet,
      skippedFields,
      hdfsSiteConf,
      coreSiteConf,
      localExportPath + s"/$datasetId"
    )

    // shutdown spark session
    spark.close()

    // package ZIP
    createZip(
      datasetId,
      DwcTerm.Event.qualifiedName(),
      eventFields,
      occurrenceFields,
      verbatimCoreFields,
      verbatimExtensionsForMeta,
      registryUrl,
      localExportPath
    )

    log.info(s"Export complete. Export in $localExportPath/${datasetId}.zip")
  }

  private def exportVerbatimCore(
      spark: SparkSession,
      verbatimDF: Dataset[Row],
      filterDownloadDF: DataFrame,
      hdfsSiteConf: String,
      coreSiteConf: String,
      localExportPath: String
  ) = {

    val dfWithExtensions = filterDownloadDF.join(
      verbatimDF,
      col("Core.id") === col("Verbatim.id"),
      "inner"
    )

    val coreFields =
      getCoreFields(dfWithExtensions, spark).filter(!_.endsWith("eventID"))
    val columns = Array(col("core.id").as("eventID")) ++ coreFields.map { fieldName =>
      col("coreTerms.`" + fieldName + "`")
        .as(fieldName.substring(fieldName.lastIndexOf("/") + 1))
    }
    val coreForExportDF = dfWithExtensions.select(columns: _*)
    coreForExportDF
      .select("*")
      .coalesce(1)
      .write
      .option("header", "true")
      .option("sep", "\t")
      .mode("overwrite")
      .csv(s"$localExportPath/Verbatim_Event")
    cleanupFileExport(
      "Verbatim_Event",
      hdfsSiteConf,
      coreSiteConf,
      localExportPath
    )

    coreFields
  }

  private def exportVerbatimExtensions(
      spark: SparkSession,
      verbatimDF: Dataset[Row],
      filterDownloadDF: DataFrame,
      hdfsSiteConf: String,
      coreSiteConf: String,
      localExportPath: String
  ) = {

    val dfWithExtensions = filterDownloadDF.join(
      verbatimDF,
      col("Core.id") === col("Verbatim.id"),
      "inner"
    )

    val extensionsForMeta =
      scala.collection.mutable.Map[String, Array[String]]()

    // get list of extensions for this dataset
    val extensionList = getExtensionList(dfWithExtensions, spark)

    // export all supplied extensions verbatim
    extensionList.foreach(extensionURI => {

      val extensionFields =
        getExtensionFields(dfWithExtensions, extensionURI, spark)

      val arrayStructureSchema = {
        var builder = new StructType().add("id", StringType)
        extensionFields.foreach { fieldName =>
          val isURI = fieldName.lastIndexOf("/") > 0
          val simpleName = if (isURI) {
            fieldName.substring(fieldName.lastIndexOf("/") + 1)
          } else {
            fieldName
          }
          builder = builder.add(simpleName, StringType)
        }
        builder
      }

      val extensionDF = dfWithExtensions
        .select(
          col("Core.id").as("id"),
          col(s"""extensions.`${extensionURI}`""").as("the_extension")
        )
        .toDF
      val rowRDD = extensionDF.rdd
        .map(row => genericRecordToRow(row, extensionFields, arrayStructureSchema))
        .flatMap(list => list)
      val extensionForExportDF =
        spark.sqlContext.createDataFrame(rowRDD, arrayStructureSchema)

      // filter "coreTerms", "extensions"
      val extensionSimpleName =
        extensionURI.substring(extensionURI.lastIndexOf("/") + 1)

      extensionForExportDF
        .select("*")
        .coalesce(1)
        .write
        .option("header", "true")
        .option("sep", "\t")
        .mode("overwrite")
        .csv(s"$localExportPath/Verbatim_$extensionSimpleName")

      cleanupFileExport(
        "Verbatim_" + extensionSimpleName,
        hdfsSiteConf,
        coreSiteConf,
        localExportPath
      )
      extensionsForMeta(extensionURI) = extensionFields
    })

    extensionsForMeta.toMap
  }

  private def exportInterpretedOccurrence(
      datasetId: String,
      hdfsPath: String,
      attempt: Int,
      spark: SparkSession,
      filterDownloadDF: DataFrame,
      extensionList: Set[String],
      skippedFields: Array[String],
      hdfsSiteConfig: String,
      coreSiteConfig: String,
      localExportPath: String
  ) = {
    // If an occurrence extension was supplied
    if (extensionList.contains(DwcTerm.Occurrence.qualifiedName())) {

      log.info("Load basic  for occurrences")
      val occBasicDF = spark.read
        .format("avro")
        .load(s"${hdfsPath}/${datasetId}/${attempt}/occurrence/basic/*.avro")
        .as("OccBasic")
        .filter("coreId is NOT NULL")

      log.info("Load occurrences")
      val occTaxonDF = spark.read
        .format("avro")
        .load(
          s"${hdfsPath}/${datasetId}/${attempt}/occurrence/ala_taxonomy/*.avro"
        )
        .as("OccTaxon")
        .filter("coreId is NOT NULL")

      log.info("Load temporal  for occurrences")
      val occTemporalDF = spark.read
        .format("avro")
        .load(s"${hdfsPath}/${datasetId}/${attempt}/occurrence/temporal/*.avro")
        .as("OccTemporal")
        .filter("coreId is NOT NULL")

      log.info("Load location for occurrences")
      val occLocationDF = spark.read
        .format("avro")
        .load(s"${hdfsPath}/${datasetId}/${attempt}/occurrence/location/*.avro")
        .as("OccLocation")
        .filter("coreId is NOT NULL")

      log.info("Create occurrence join DF")
      val occDF = occBasicDF
        .join(occTaxonDF, col("OccBasic.id") === col("OccTaxon.id"), "inner")
        .join(
          occLocationDF,
          col("OccBasic.id") === col("OccLocation.id"),
          "inner"
        )
        .join(
          occTemporalDF,
          col("OccBasic.id") === col("OccTemporal.id"),
          "inner"
        )

      val joinOccDF = filterDownloadDF
        .select(col("Core.id"))
        .join(occDF, col("Core.id") === col("OccBasic.coreId"), "inner")

      log.info("Generate interpreted occurrence DF for export")
      val (exportDF, fields) =
        generateInterpretedExportDF(
          joinOccDF,
          skippedFields,
          Array("OccBasic", "OccTaxon", "OccLocation", "OccTemporal")
        )

      log.info("Export interpreted occurrence data")
      exportDF.write
        .option("header", "true")
        .option("sep", "\t")
        .mode("overwrite")
        .csv(s"$localExportPath/Occurrence")

      cleanupFileExport(
        "Occurrence",
        hdfsSiteConfig,
        coreSiteConfig,
        localExportPath
      )

      fields
    } else {
      Array[String]()
    }
  }

  def generateInterpretedExportDF(
      df: DataFrame,
      skippedFields: Array[String],
      issuesAliases: Array[String]
  ): (DataFrame, Array[String]) = {

    val primitiveFields = df.schema.fields.filter(structField => {
      if (
        structField.dataType.isInstanceOf[StringType]
        || structField.dataType.isInstanceOf[DoubleType]
        || structField.dataType.isInstanceOf[IntegerType]
        || structField.dataType.isInstanceOf[LongType]
        || structField.dataType.isInstanceOf[DecimalType]
        || structField.dataType.isInstanceOf[BooleanType]
      ) true
      else false
    })

    val stringArrayFields = df.schema.fields.filter(structField => {
      if (structField.dataType.isInstanceOf[ArrayType]) {
        val arrayType = structField.dataType.asInstanceOf[ArrayType]
        if (arrayType.elementType.isInstanceOf[StringType]) {
          true
        } else {
          false
        }
      } else {
        false
      }
    })

    val issues = issuesAliases.map(alias => col(s"$alias.issues").as(s"${alias}_issues"))

    val exportFields = (primitiveFields.map { field =>
      field.name
    } ++ stringArrayFields.map { field => field.name })
      .filter(!skippedFields.contains(_))

    val fields =
      Array(col("core.id").as("eventID")) ++ exportFields.map(col(_)) ++ issues

    var occDFCoalesce = df.select(fields: _*).coalesce(1)

    stringArrayFields.foreach { arrayField =>
      occDFCoalesce = occDFCoalesce.withColumn(
        arrayField.name,
        concat_ws(";", col(arrayField.name))
      )
    }

    issuesAliases.foreach { alias =>
      occDFCoalesce = occDFCoalesce.withColumn(
        s"${alias}_issues",
        concat_ws(";", col(s"${alias}_issues.issueList"))
      )
    }

    (occDFCoalesce, Array("Core.id") ++ exportFields ++ issuesAliases.map(alias => s"${alias}_issues").toArray[String])
  }

  val META_XML_ENCODING = "UTF-8"

  def save(node: Node, fileName: String) {
    val pp = new PrettyPrinter(120, 2)
    val fos = new FileOutputStream(fileName)
    val writer = Channels.newWriter(fos.getChannel(), META_XML_ENCODING)
    try {
      writer.write(
        "<?xml version='1.0' encoding='" + META_XML_ENCODING + "'?>\n"
      )
      writer.write(pp.format(node))
    } finally {
      writer.close()
    }
  }

  private def createZip(
      datasetId: String,
      coreTermType: String,
      coreFieldList: Array[String],
      occurrenceFieldList: Array[String],
      verbatimCoreFields: Array[String],
      extensionsForMeta: Map[String, Array[String]],
      registryUrl: String,
      localExportPath: String
  ) = {

    // write the XML
    val metaXml = createMeta(
      coreTermType,
      coreFieldList,
      occurrenceFieldList,
      verbatimCoreFields,
      extensionsForMeta
    )
    save(metaXml, s"$localExportPath/$datasetId/meta.xml")

    // get EML doc
    import sys.process._
    val registryUrlClean = if (registryUrl.endsWith("/")) registryUrl else registryUrl + "/"
    new URL(s"${registryUrlClean}eml/${datasetId}") #> new File(
      s"$localExportPath/$datasetId/eml.xml"
    ) !!

    // create a zip
    val zip = new ZipOutputStream(
      new FileOutputStream(
        new File(s"$localExportPath/${datasetId}.zip")
      )
    )
    new File(s"$localExportPath/$datasetId").listFiles().foreach { file =>
      if (!file.getName.endsWith(datasetId + ".zip")) {
        log.info("Zipping " + file.getName)
        zip.putNextEntry(new ZipEntry(file.getName))
        Files.copy(file.toPath, zip)
        zip.flush()
        zip.closeEntry()
      }
    }
    zip.flush()
    zip.close()
  }

  def generateFieldColumns(fields: Seq[String]): Seq[Column] = {
    fields.map {
      case "core.id"           => col("core.id").as("id")
      case "Core.id"           => col("core.id").as("id")
      case "eventDate.gte"     => col("eventDate.gte").as("eventDate")
      case "eventType.concept" => col("eventType.concept").as("eventType")
      case x                   => col("" + x).as(x)
    }
  }

  def generateCoreFieldMetaName(field: String): String = {
    if (field.startsWith("http")) {
      field
    } else {
      field match {
        case "id"                 => "http://rs.tdwg.org/dwc/terms/eventID"
        case "core.id"            => "http://rs.tdwg.org/dwc/terms/eventID"
        case "Core_issues"        => "http://rs.tdwg.org/dwc/terms/issues"
        case "Temporal_issues"    => "http://rs.tdwg.org/dwc/terms/temporalIssues"
        case "Location_issues"    => "http://rs.tdwg.org/dwc/terms/locationIssues"
        case "OccBasic_issues"    => "http://rs.tdwg.org/dwc/terms/issues"
        case "OccTemporal_issues" => "http://rs.tdwg.org/dwc/terms/temporalIssues"
        case "OccTaxon_issues"    => "http://rs.tdwg.org/dwc/terms/taxonomicIssues"
        case "OccLocation_issues" => "http://rs.tdwg.org/dwc/terms/locationIssues"
        case "eventDate.gte"      => "http://rs.tdwg.org/dwc/terms/eventDate"
        case "eventType.concept"  => "http://rs.gbif.org/terms/1.0/eventType"
        case "elevationAccuracy" =>
          "http://rs.gbif.org/terms/1.0/elevationAccuracy"
        case "depthAccuracy" => "http://rs.gbif.org/terms/1.0/depthAccuracy"
        case x               => "http://rs.tdwg.org/dwc/terms/" + x
      }
    }
  }

  /** Clean up the file export, moving to sensible files names instead of the part-* file name generated by spark.
    *
    * @param jobID
    * @param extensionSimpleName
    */
  private def cleanupFileExport(
      extensionSimpleName: String,
      hdfsSiteConf: String,
      coreSiteConf: String,
      localExportPath: String
  ) = {

    log.info("Checking for " + s"$localExportPath/${extensionSimpleName}")

    val localFile = new File(
      s"$localExportPath/${extensionSimpleName}"
    )

    if (localFile.exists()) {
      log.info("Local file exists = " + localFile.getPath)
    } else {
      val conf = new Configuration
      conf.addResource(new File(hdfsSiteConf).toURI().toURL())
      conf.addResource(new File(coreSiteConf).toURI().toURL())
      val hdfsPrefixToUse = conf.get("fs.defaultFS")
      val hdfsFs = FileSystem.get(URI.create(hdfsPrefixToUse), conf)

      log.info("Trying to copy to Local = " + localFile.getPath)
      // copy to local file system
      hdfsFs.copyToLocalFile(
        new Path(
          hdfsPrefixToUse + s"$localExportPath/$extensionSimpleName"
        ),
        new Path(s"$localExportPath/$extensionSimpleName")
      )
    }

    // move part-* file to {extension_name}.txt
    log.info("Cleaning up extension " + extensionSimpleName)

    val file = new File(s"$localExportPath/$extensionSimpleName")
    val outputFile = file.listFiles
      .filter(exportFile => exportFile != null && exportFile.isFile)
      .filter(_.getName.startsWith("part-"))
      .map(_.getPath)
      .toList
      .head

    // move to sensible name
    FileUtils.moveFile(
      new File(outputFile),
      new File(
        s"$localExportPath/${extensionSimpleName.toLowerCase()}.txt"
      )
    )

    // remote temporary directory
    FileUtils.forceDelete(
      new File(s"$localExportPath/$extensionSimpleName")
    )
  }

  def generateInterpretedExtension(extensionUri: String, extensionFields: Array[String]): Elem = {
    val extensionFileName =
      extensionUri.substring(extensionUri.lastIndexOf("/") + 1).toLowerCase
    generateInterpretedExtension(extensionUri, extensionFileName, extensionFields)
  }

  def generateInterpretedExtension(
      extensionUri: String,
      extensionFileName: String,
      extensionFields: Array[String]
  ): Elem = {
    <extension rowType={extensionUri} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
      <files>
        <location>{extensionFileName}.txt</location>
      </files>
      <coreid index="0"/>{
      extensionFields.zipWithIndex.map {
        case (uri, fieldIdx) => {
          <field index={fieldIdx.toString} term={
            generateCoreFieldMetaName(uri)
          }/>
        }
      }
    }
    </extension>
  }

  def generateVerbatimExtension(
      extensionUri: String,
      extensionFields: Array[String]
  ): Elem = {
    val extensionFileName =
      extensionUri.substring(extensionUri.lastIndexOf("/") + 1).toLowerCase
    generateVerbatimExtension(extensionUri, "verbatim_" + extensionFileName, extensionFields)
  }

  def generateVerbatimExtension(
      extensionUri: String,
      extensionFileName: String,
      extensionFields: Array[String]
  ): Elem = {
    <extension rowType={extensionUri} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
      <files>
        <location>{extensionFileName}.txt</location>
      </files>
      <coreid index="0"/>{
      if (false) <field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/>
    }<field index="0" term="http://rs.tdwg.org/dwc/terms/eventID"/>{
      extensionFields.zipWithIndex.map {
        case (uri, fieldIdx) => {
          <field index={(fieldIdx.toInt + 1).toString} term={
            generateCoreFieldMetaName(uri)
          }/>
        }
      }
    }
    </extension>
  }

  def createMeta(
      coreURI: String,
      coreFields: Seq[String],
      occurrenceFields: Array[String],
      verbatimCoreFields: Array[String],
      extensionsForMeta: Map[String, Array[String]]
  ): Elem = {
    val coreFileName =
      coreURI.substring(coreURI.lastIndexOf("/") + 1).toLowerCase
    val metaXml = <archive xmlns="http://rs.tdwg.org/dwc/text/">
      <core rowType={coreURI} encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\r\n" fieldsEnclosedBy="&quot;" ignoreHeaderLines="1">
        <files>
          <location>{coreFileName}.txt</location>
        </files>
        <id index="0"/>{
      coreFields.zipWithIndex.map { case (uri, index) =>
        <field index={index.toString} term={generateCoreFieldMetaName(uri)}/>
      }
    }
      </core>{
      generateInterpretedExtension(
        "http://ala.org.au/terms/1.0/VerbatimOccurrence",
        "verbatim_occurrence",
        occurrenceFields
      )
    }{
      generateVerbatimExtension(
        "http://ala.org.au/terms/1.0/VerbatimEvent",
        "verbatim_event",
        verbatimCoreFields
      )
    }{
      extensionsForMeta.map { case (extensionUri, fields) =>
        generateVerbatimExtension(extensionUri, fields)
      }
    }
    </archive>
    metaXml
  }

  def genericRecordToRow(row: Row, extensionFields: Array[String], sqlType: StructType): Seq[Row] = {
    val coreID = row.get(0).asInstanceOf[String]
    val elements = row.get(1).asInstanceOf[Seq[Map[String, String]]]
    elements.map(record => {
      val values = extensionFields
        .map(fieldName => record.getOrElse(fieldName, ""))
        .toArray[Any]
      new GenericRowWithSchema(Array(coreID) ++ values, sqlType)
    })
  }

  def getExtensionList(joined_df: DataFrame, spark: SparkSession): Array[String] = {
    val fieldNameStructureSchema = new StructType()
      .add("fieldName", StringType)

    val extensionsDF =
      joined_df.select(col(s"""extensions""").as("the_extensions")).toDF

    val rowRDD = extensionsDF.rdd
      .map(row => extensionFieldNameRow(row, fieldNameStructureSchema))
      .flatMap(list => list)

    val fieldNameDF =
      spark.sqlContext.createDataFrame(rowRDD, fieldNameStructureSchema)

    val rows = fieldNameDF.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def getCoreFields(verbatimDF: DataFrame, spark: SparkSession): Array[String] = {

    import org.apache.spark.sql.types._
    val fieldNameStructureSchema = new StructType().add("fieldName", StringType)

    val coreDF = verbatimDF.select(col(s"""coreTerms""").as("coreTerms")).toDF

    val rowRDD = coreDF.rdd
      .map { row =>
        coreRecordToFieldNameRow(row, fieldNameStructureSchema)
      }
      .flatMap(list => list)

    val df = spark.sqlContext.createDataFrame(rowRDD, fieldNameStructureSchema)
    val rows = df.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def getExtensionFields(joined_df: DataFrame, extension: String, spark: SparkSession): Array[String] = {
    val fieldNameStructureSchema = new StructType()
      .add("fieldName", StringType)

    val extensionDF = joined_df
      .select(col(s"""extensions.`${extension}`""").as("the_extension"))
      .toDF

    val rowRDD = extensionDF.rdd
      .map(row => genericRecordToFieldNameRow(row, fieldNameStructureSchema))
      .flatMap(list => list)
    val df = spark.sqlContext.createDataFrame(rowRDD, fieldNameStructureSchema)
    val rows = df.distinct().select(col("fieldName")).head(1000)
    rows.map(_.getString(0))
  }

  def coreRecordToFieldNameRow(row: Row, sqlType: StructType): Seq[Row] = {
    val extensionUris = row.get(0).asInstanceOf[Map[String, Any]].keySet
    extensionUris
      .map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType))
      .toSeq
  }

  def extensionFieldNameRow(row: Row, sqlType: StructType): Seq[Row] = {
    val extensionUris = row.get(0).asInstanceOf[Map[String, Any]].keySet
    extensionUris
      .map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType))
      .toSeq
  }

  def genericCoreRecordToFieldNameRow(row: Row, sqlType: StructType): Set[GenericRowWithSchema] = {
    val elements = row.get(0).asInstanceOf[Map[String, String]]
    val fieldNames = elements.keySet.flatten
    fieldNames.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType))
  }

  def genericRecordToFieldNameRow(row: Row, sqlType: StructType): Seq[Row] = {
    val elements = row.get(0).asInstanceOf[Seq[Map[String, String]]]
    val fieldNames = elements.map(record => record.keySet).flatten
    fieldNames.distinct.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType))
  }
}
