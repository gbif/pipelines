package au.org.ala.pipelines.spark

import au.org.ala.kvs.{ALAPipelinesConfig, ALAPipelinesConfigFactory}

import _root_.java.io._
import _root_.java.net._
import _root_.java.util.{UUID}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, LocatedFileStatus, Path, RemoteIterator}
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
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.gbif.api.model.common.search.SearchParameter
import org.gbif.api.model.predicate.Predicate
import org.gbif.dwc.terms.DwcTerm
import au.org.ala.predicate.{ALAEventSearchParameter, ALAEventSparkQueryVisitor, ALAEventTermsMapper}
import au.org.ala.utils.CombinedYamlConfiguration
import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.google.common.io.ByteStreams
import org.gbif.hadoop.compress.d2.{D2CombineInputStream, D2Utils}
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream
import org.gbif.pipelines.core.pojo.HdfsConfigs
import org.slf4j.LoggerFactory

import java.nio.channels.Channels
import java.util
import scala.xml.{Elem, Node, PrettyPrinter}

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

    val jobID = createJobID(exportArgs)

    // Process the query filter
    val queryFilter = constructQuery(exportArgs)

    val hdfsConfigs = HdfsConfigs.create(exportArgs.hdfsSiteConfig, exportArgs.coreSiteConfig)
    val config: ALAPipelinesConfig = ALAPipelinesConfigFactory.getInstance(hdfsConfigs, exportArgs.properties).get

    log.info(s"Export for ${exportArgs.datasetId} - jobId ${jobID}")
    log.info(s"Generated query: $queryFilter")

    val localExportPath = createExportPath(exportArgs)
    log.info("Local export path = " + localExportPath)

    runExport(
      exportArgs.datasetId,
      exportArgs.inputPath,
      createTargetExportPath(exportArgs),
      localExportPath,
      exportArgs.attempt,
      queryFilter,
      exportArgs.skipEventExportFields.toArray(Array[String]()),
      exportArgs.skipOccurrenceExportFields.toArray(Array[String]()),
      exportArgs.hdfsSiteConfig,
      exportArgs.coreSiteConfig,
      config.collectory.getWsUrl()
    )
  }

  private def createJobID(exportArgs: CmdArgs) = {
    if (exportArgs.jobId != null && !exportArgs.jobId.isEmpty) {
      exportArgs.jobId
    } else {
      UUID.randomUUID.toString
    }
  }

  private def createTargetExportPath(exportArgs: CmdArgs) = {
    if (exportArgs.jobId != null && !exportArgs.jobId.isEmpty) {
      exportArgs.targetPath + "/" + exportArgs.jobId + "/" + exportArgs.datasetId
    } else {
      exportArgs.targetPath + "/" + exportArgs.datasetId
    }
  }

  private def createExportPath(exportArgs: CmdArgs) = {
    if (exportArgs.jobId != null && !exportArgs.jobId.isEmpty) {
      exportArgs.localExportPath + "/" + exportArgs.jobId
    } else {
      exportArgs.localExportPath
    }
  }

  private def constructQuery(exportArgs: CmdArgs) = {
    if (exportArgs.queryFilePath != null && !exportArgs.queryFilePath.isEmpty) {
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
  }

  def runExport(
      datasetId: String,
      inputPath: String,
      targetPath: String,
      localExportPath: String,
      attempt: Int,
      queryFilter: String,
      skipEventExportFields: Array[String],
      skipOccurrenceExportFields: Array[String],
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

    // get a list columns
    log.info("Load search index")
    val filterSearchDF = {

      val eventSearchDF = spark.read
        .format("avro")
        .load(s"${inputPath}/${datasetId}/${attempt}/search/event/*.avro")
        .as("Search")

      if (queryFilter != "") {
        // filter "coreTerms", "extensions"
        eventSearchDF.filter(queryFilter).toDF()
      } else {
        eventSearchDF
      }
    }

    // generate interpreted event export
    log.info("Export interpreted event data")
    val (eventExportDF, eventFields) = generateInterpretedExportDF(filterSearchDF, skipEventExportFields)

    eventExportDF.write
      .option("header", "true")
      .option("sep", "\t")
      .mode("overwrite")
      .option("codec", "org.gbif.hadoop.compress.d2.D2Codec")
      .csv(s"$targetPath/Event/")

    // export interpreted occurrence
    val (occurrenceFields, verbatimOccurrenceFields) = exportOccurrence(
      datasetId,
      inputPath,
      targetPath,
      attempt,
      spark,
      filterSearchDF,
      skipOccurrenceExportFields
    )

    // load the verbatim DF
    val verbatimDF = spark.read
      .format("avro")
      .load(s"${inputPath}/${datasetId}/${attempt}/verbatim/*.avro")
      .as("Verbatim")

    val verbatimDFJoined = filterSearchDF.join(
      verbatimDF,
      col("Search.id") === col("Verbatim.id"),
      "inner"
    )

    // export the supplied core verbatim
    val verbatimCoreFields = exportVerbatimCore(
      spark,
      verbatimDFJoined,
      targetPath
    )

    // export the supplied extensions verbatim
    val verbatimExtensionsForMeta = exportVerbatimExtensions(
      spark,
      verbatimDFJoined,
      targetPath
    )

    // shutdown spark session
    spark.close()

    // package ZIP
    createZip(
      hdfsSiteConf,
      coreSiteConf,
      targetPath: String,
      datasetId,
      DwcTerm.Event.qualifiedName(),
      eventFields,
      occurrenceFields,
      verbatimCoreFields,
      verbatimOccurrenceFields,
      verbatimExtensionsForMeta,
      registryUrl,
      localExportPath
    )

    log.info(s"Export complete. Export in $localExportPath/${datasetId}.zip")
  }

  private def exportVerbatimCore(
      spark: SparkSession,
      verbatimDFJoined: DataFrame,
      targetPath: String
  ) = {

    val coreFields =
      getCoreFields(verbatimDFJoined, spark).filter(!_.endsWith("eventID"))
    val columns = Array(col("Search.id").as("eventID")) ++ coreFields.map { fieldName =>
      col("coreTerms.`" + fieldName + "`")
        .as(fieldName.substring(fieldName.lastIndexOf("/") + 1))
    }
    val coreForExportDF = verbatimDFJoined.select(columns: _*)
    coreForExportDF
      .select("*")
      .write
      .option("header", "true")
      .option("sep", "\t")
      .mode("overwrite")
      .option("codec", "org.gbif.hadoop.compress.d2.D2Codec")
      .csv(s"$targetPath/Verbatim_Event")

    coreFields
  }

  private def exportVerbatimExtensions(
      spark: SparkSession,
      verbatimDFJoined: DataFrame,
      targetPath: String
  ) = {

    val extensionsForMeta =
      scala.collection.mutable.Map[String, Array[String]]()

    // get list of extensions for this dataset
    val extensionList = getExtensionList(verbatimDFJoined, spark)

    // export all supplied extensions verbatim
    extensionList.foreach(extensionURI => {

      if (!extensionURI.equals(DwcTerm.Occurrence.qualifiedName())) {

        val extensionFields =
          getExtensionFields(verbatimDFJoined, extensionURI, spark)

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

        val extensionDF = verbatimDFJoined
          .select(
            col("Search.id").as("id"),
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
          .write
          .option("header", "true")
          .option("sep", "\t")
          .mode("overwrite")
          .option("codec", "org.gbif.hadoop.compress.d2.D2Codec")
          .csv(s"$targetPath/Verbatim_$extensionSimpleName")

        extensionsForMeta(extensionURI) = extensionFields
      }
    })

    extensionsForMeta.toMap
  }

  def sanitise(fieldName: String): String =
    if (fieldName.lastIndexOf("/") > 0)
      fieldName.substring(fieldName.lastIndexOf("/") + 1)
    else
      fieldName

  private def exportOccurrence(
      datasetId: String,
      inputPath: String,
      targetPath: String,
      attempt: Int,
      spark: SparkSession,
      filterDownloadDF: DataFrame,
      skippedFields: Array[String]
  ): (Array[String], Array[String]) = {
    // If an occurrence extension was supplied
    log.info("Create occurrence join DF")
    val occDF = spark.read
      .format("avro")
      .load(s"${inputPath}/${datasetId}/${attempt}/search/occurrence/*.avro")
      .as("Occurrence")
      .filter("coreId is NOT NULL")

    val joinOccDF = filterDownloadDF
      .select(col("Search.id"))
      .join(occDF, col("Search.id") === col("Occurrence.coreId"), "inner")

    log.info("Generate interpreted occurrence DF for export")
    val (exportDF, fields) =
      generateInterpretedExportDF(
        joinOccDF,
        skippedFields
      )

    log.info("Export interpreted occurrence data")
    exportDF.write
      .option("header", "true")
      .option("sep", "\t")
      .mode("overwrite")
      .option("codec", "org.gbif.hadoop.compress.d2.D2Codec")
      .csv(s"$targetPath/Occurrence")

    // Export verbatim occurrence records
    val fieldNameStructureSchema = new StructType()
      .add("fieldName", StringType)
    val rowRDD = joinOccDF
      .select(col("verbatim"))
      .rdd
      .map(row => genericRecordFieldToFieldNameRow(row, fieldNameStructureSchema))
      .flatMap(list => list)
    val df = spark.sqlContext.createDataFrame(rowRDD, fieldNameStructureSchema)
    val rows = df.distinct().select(col("fieldName")).head(1000)
    val verbatimFieldNames = rows.map(_.getString(0))
    val colsToSelect =
      Array(col("Search.id")) ++ verbatimFieldNames.map(fieldName =>
        col("verbatim.`" + fieldName + "`").as(sanitise(fieldName))
      )

    joinOccDF
      .select(colsToSelect: _*)
      .write
      .option("header", "true")
      .option("sep", "\t")
      .mode("overwrite")
      .option("codec", "org.gbif.hadoop.compress.d2.D2Codec")
      .csv(s"$targetPath/Verbatim_Occurrence")

    (fields, verbatimFieldNames)
  }

  def generateInterpretedExportDF(
      df: DataFrame,
      skippedFields: Array[String]
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

    val exportFields = (primitiveFields.map { field =>
      field.name
    } ++ stringArrayFields.map { field => field.name })
      .filter(!skippedFields.contains(_))

    val fields =
      Array(col("Search.id").as("eventID")) ++ exportFields.map(col(_))

    var occDFCoalesce = df.select(fields: _*).coalesce(1)

    stringArrayFields.foreach { arrayField =>
      occDFCoalesce = occDFCoalesce.withColumn(
        arrayField.name,
        concat_ws(";", col(arrayField.name))
      )
    }

    (
      occDFCoalesce,
      Array("Search.id") ++ exportFields
    )
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
      hdfsSiteConf: String,
      coreSiteConf: String,
      targetPath: String,
      datasetId: String,
      coreTermType: String,
      coreFieldList: Array[String],
      occurrenceFieldList: Array[String],
      verbatimCoreFields: Array[String],
      verbatimOccurrenceFields: Array[String],
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
      verbatimOccurrenceFields,
      extensionsForMeta
    )

    log.info(s"Creating export directory meta XML to path: $localExportPath/$datasetId")
    FileUtils.forceMkdir(new File(s"$localExportPath/$datasetId"))

    val metaXmlPath = s"$localExportPath/$datasetId/meta.xml"
    log.info(s"Writing meta XML to path: $metaXmlPath")
    save(metaXml, metaXmlPath)

    // get EML doc
    import sys.process._
    val emlXmlPath = s"$localExportPath/$datasetId/eml.xml"
    log.info(s"Writing EML XML to path: $emlXmlPath")
    val registryUrlClean = if (registryUrl.endsWith("/")) registryUrl else registryUrl + "/"
    new URL(s"${registryUrlClean}eml/${datasetId}") #> new File(
      emlXmlPath
    ) !!

    val sourceFs: FileSystem = {
      FileSystem.get({
        if (!targetPath.startsWith("hdfs:")) {
          new Configuration()
        } else {
          val conf = new Configuration
          conf.addResource(new File(hdfsSiteConf).toURI().toURL())
          conf.addResource(new File(coreSiteConf).toURI().toURL())
          conf
        }
      })
    }

    val targetFs: FileSystem = FileSystem.getLocal(new Configuration())
    val zipPath = localExportPath + "/" + datasetId + ".zip"
    log.info(s"Writing Zip to path: $zipPath")
    val zipped = targetFs.create(new Path(zipPath), true)
    val out = new ModalZipOutputStream(new BufferedOutputStream(zipped, 10 * 1024 * 1024))

    val fileStatuses: Array[FileStatus] =
      sourceFs.listStatus(new Path(targetPath))

    fileStatuses.foreach { fs =>
      val path = fs.getPath
      if (sourceFs.isDirectory(path)) {
        addZipEntry(sourceFs, targetPath, path.getName, out)
      }
    }

    // add meta
    out.putNextEntry(new org.gbif.hadoop.compress.d2.zip.ZipEntry("meta.xml"), ModalZipOutputStream.MODE.DEFAULT)
    ByteStreams.copy(new FileInputStream(new File(metaXmlPath)), out)
    out.flush()
    out.closeEntry()

    // add eml.xml
    out.putNextEntry(new org.gbif.hadoop.compress.d2.zip.ZipEntry("eml.xml"), ModalZipOutputStream.MODE.DEFAULT)
    ByteStreams.copy(new FileInputStream(new File(emlXmlPath)), out)
    out.flush()
    out.closeEntry()

    out.flush()
    out.close()
  }

  def addZipEntry(
      sourceFs: FileSystem,
      targetPath: String,
      extension: String,
      out: ModalZipOutputStream
  ): Unit = {

    val parts = new util.ArrayList[InputStream]()
    val remoteFile: RemoteIterator[LocatedFileStatus] =
      sourceFs.listFiles(new Path(targetPath + "/" + extension), false)

    while ({ remoteFile.hasNext }) {
      val fs = remoteFile.next
      val path = fs.getPath
      if (path.toString.endsWith(D2Utils.FILE_EXTENSION)) {
        log.info("Deflated content to merge: {} ", path)
        parts.add(sourceFs.open(path))
      }
    }

    // create the Zip entry, and write the compressed bytes
    val ze = new org.gbif.hadoop.compress.d2.zip.ZipEntry(extension.toLowerCase() + ".txt")
    out.putNextEntry(ze, ModalZipOutputStream.MODE.PRE_DEFLATED)
    val in = new D2CombineInputStream(parts)
    ByteStreams.copy(in, out)
    in.close(); // important so counts are accurate
    ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
    ze.setCompressedSize(in.getCompressedLength());
    ze.setCrc(in.getCrc32());
    out.closeEntry();
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
        case "id"                => "http://rs.tdwg.org/dwc/terms/eventID"
        case "Search.id"         => "http://rs.tdwg.org/dwc/terms/eventID"
        case "core.id"           => "http://rs.tdwg.org/dwc/terms/eventID"
        case "issues"            => "http://rs.tdwg.org/dwc/terms/issues"
        case "Occurrence_issues" => "http://rs.tdwg.org/dwc/terms/issues"
        case "Event_issues"      => "http://rs.tdwg.org/dwc/terms/issues"
        case "Search_issues"     => "http://rs.tdwg.org/dwc/terms/issues"
        case "eventDate.gte"     => "http://rs.tdwg.org/dwc/terms/eventDate"
        case "eventType.concept" => "http://rs.gbif.org/terms/1.0/eventType"
        case "elevationAccuracy" =>
          "http://rs.gbif.org/terms/1.0/elevationAccuracy"
        case "depthAccuracy" => "http://rs.gbif.org/terms/1.0/depthAccuracy"
        case x               => "http://rs.tdwg.org/dwc/terms/" + x
      }
    }
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
      verbatimOccurrenceFields: Array[String],
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
        DwcTerm.Occurrence.qualifiedName(),
        "occurrence",
        occurrenceFields
      )
    }{
      generateVerbatimExtension(
        "http://ala.org.au/terms/1.0/VerbatimEvent",
        "verbatim_event",
        verbatimCoreFields
      )
    }{
      generateVerbatimExtension(
        "http://ala.org.au/terms/1.0/VerbatimOccurrence",
        "verbatim_occurrence",
        verbatimOccurrenceFields
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

  def genericRecordFieldToFieldNameRow(row: Row, sqlType: StructType): Seq[Row] = {
    val elements = row.get(0).asInstanceOf[Map[String, String]]
    val fieldNames = elements.keySet
    fieldNames.map(fieldName => new GenericRowWithSchema(Array(fieldName), sqlType)).toSeq
  }
}
