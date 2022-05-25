package org.gbif.pipelines.clustering

import java.io.File

import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, HFileOutputFormat2}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partitioner
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.gbif.pipelines.core.parsers.clustering.{OccurrenceRelationships, RelationshipAssertion}

import scala.collection.JavaConversions._

object Cluster {

  /**
   * Reads the salt from the encoded key structure.
   */
  class SaltPartitioner(partitionCount: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      // (salt:id1:id2:type, column)
      val k = key.asInstanceOf[(String, String)]._1
      Integer.valueOf(k.substring(0, k.indexOf(":")))
    }

    override def numPartitions: Int = partitionCount
  }

  // To aid running in Oozie, all properties are supplied as main arguments
  val usage = """
    Usage: Cluster \
      [--hive-db database] \
      [--hive-table-hashed tableName] \
      [--hive-table-candidates tableName] \
      [--hive-table-relationships tableName] \
      [--hbase-table tableName] \
      [--hbase-regions numberOfRegions] \
      [--hbase-zk zookeeperEnsemble] \
      [--hfile-dir directoryForHFiles]
  """

  def main(args: Array[String]): Unit = {
    val parsedArgs = checkArgs(args) // sanitize input
    assert(parsedArgs.size==8, usage)
    System.err.println("Configuration: " + parsedArgs) // Oozie friendly logging use

    val hiveDatabase = parsedArgs.get('hiveDatabase).get
    val hiveTableHashed = parsedArgs.get('hiveTableHashed).get
    val hiveTableCandidates = parsedArgs.get('hiveTableCandidates).get
    val hiveTableRelationships = parsedArgs.get('hiveTableRelationships).get
    val hbaseTable = parsedArgs.get('hbaseTable).get
    val hbaseRegions = parsedArgs.get('hbaseRegions).get.toInt
    val hbaseZK = parsedArgs.get('hbaseZK).get
    val hfileDir = parsedArgs.get('hfileDir).get

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Occurrence clustering")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql

    spark.sql("use " + hiveDatabase)

    val runAll = true; // developers: set to false to short circuit to the clustering stage

    if (runAll) {
      val occurrences = sql(SQL_OCCURRENCE)

      val schema = StructType(
        StructField("gbifId", LongType, nullable = false) ::
          StructField("datasetKey", StringType, nullable = false) ::
          StructField("hash", StringType, nullable = false) :: Nil
      )
      val hashEncoder = RowEncoder(schema)

      import spark.implicits._

      // species+ids for specimens only
      val hashSpecimenIds = occurrences.flatMap(r => {
        val records = scala.collection.mutable.ListBuffer[Row]()

        // specimens often link by record identifiers, while occurrence data skews here greatly for little benefit
        val bor = Option(r.getAs[String]("basisOfRecord"))
        if (specimenBORs.contains(bor.getOrElse("ignore"))) {
          var idsUsed = Set[Option[String]](
            Option(r.getAs[String]("occurrenceID")),
            Option(r.getAs[String]("fieldNumber")),
            Option(r.getAs[String]("recordNumber")),
            Option(r.getAs[String]("catalogNumber")),
            triplify(r), // ic:cc:cn format
            scopeCatalogNumber(r) // ic:cn format like ENA uses
          )

          val other = Option(r.getAs[Seq[String]]("otherCatalogNumbers"))
          if (!other.isEmpty && other.get.length>0) {
            idsUsed ++= other.get.map(x => Option(x))
          }

          // clean IDs
          val filteredIds = idsUsed.filter(s => {
            s match {
              case Some(s) => s!=null && s.length>0 && !omitIds.contains(s.toUpperCase())
              case _ => false
            }
          })

          filteredIds.foreach(id => {
            id match {
              case Some(id) => {
                records.append(
                  Row(
                    r.getAs[Long]("gbifId"),
                    r.getAs[String]("datasetKey"),
                    r.getAs[Integer]("speciesKey") + "|" + OccurrenceRelationships.normalizeID(id)
                  ))
              }
              case None => // skip
            }
          })
        }
        records
      })(hashEncoder).toDF()

      val hashAll =  occurrences.flatMap(r => {
        val records = scala.collection.mutable.ListBuffer[Row]()

        // all records of species at same location, time should be compared regardless of BOR
        // TODO: consider improving this for null values (and when one side is null) - will the ID link above suffice?
        val lat = Option(r.getAs[Double]("decimalLatitude"))
        val lng = Option(r.getAs[Double]("decimalLongitude"))
        val year = Option(r.getAs[Int]("year"))
        val month = Option(r.getAs[Int]("month"))
        val day = Option(r.getAs[Int]("day"))
        val taxonKey = Option(r.getAs[Int]("taxonKey"))
        val typeStatus = Option(r.getAs[Seq[String]]("typeStatus"))
        val recordedBy = Option(r.getAs[Seq[String]]("recordedBy"))
        if (!lat.isEmpty && !lng.isEmpty && !year.isEmpty && !month.isEmpty && !day.isEmpty) {
          records.append(
            Row(
              r.getAs[Long]("gbifId"),
              r.getAs[String]("datasetKey"),
              r.getAs[Integer]("speciesKey") + "|" + Math.round(lat.get*1000) + "|" + Math.round(lng.get*1000) + "|" + year.get + "|" + month.get + "|" + day.get
            ))
        }
        // any type record of a taxon is of interest
        if (!taxonKey.isEmpty && !typeStatus.isEmpty) {
          typeStatus.get.foreach(t => {
            records.append(
              Row(
                r.getAs[Long]("gbifId"),
                r.getAs[String]("datasetKey"),
                taxonKey.get + "|" + t
              ))
          })
        }

        // all similar species recorded by the same person within the same year is of interest (misses recordings over new year)
        if (!taxonKey.isEmpty && !year.isEmpty && !recordedBy.isEmpty) {
          recordedBy.get.foreach(name => {
            records.append(
              Row(
                r.getAs[Long]("gbifId"),
                r.getAs[String]("datasetKey"),
                taxonKey.get + "|" + year.get + "|" + name
              ))
          })
        }

        records
      })(hashEncoder).toDF()

      val deduplicatedHashedRecords = hashAll.union(hashSpecimenIds).dropDuplicates()

      // persist for debugging, enable for further processing in SQL
      deduplicatedHashedRecords.write.saveAsTable(hiveTableHashed) // for diagnostics in hive
      deduplicatedHashedRecords.createOrReplaceTempView("DF_hashed")

      // Cross join to distinct pairs of records spanning 2 datasets
      val candidates = sql("""
      SELECT t1.gbifId as id1, t1.datasetKey as ds1, t2.gbifId as id2, t2.datasetKey as ds2
      FROM DF_hashed t1 JOIN DF_hashed t2 ON t1.hash = t2.hash
      WHERE
        t1.gbifId < t2.gbifId AND
        t1.datasetKey != t2.datasetKey
      GROUP BY t1.gbifId, t1.datasetKey, t2.gbifId, t2.datasetKey
      """);

      candidates.write.saveAsTable(hiveTableCandidates) // for diagnostics in hive
    }

    // Spark DF naming convention requires that we alias each term to avoid naming collision while still having
    // named fields to access (i.e. not relying the column number of the term). All taxa keys are converted to String
    // to allow shared routines between GBIF and ALA (https://github.com/gbif/pipelines/issues/484)
    val pairs = sql("""
      SELECT

        t1.gbifId AS t1_gbifId, t1.datasetKey AS t1_datasetKey, t1.basisOfRecord AS t1_basisOfRecord, t1.publishingorgkey AS t1_publishingOrgKey, t1.datasetName AS t1_datasetName, t1.publisher AS t1_publishingOrgName,
        CAST(t1.kingdomKey AS String) AS t1_kingdomKey, CAST(t1.phylumKey AS String) AS t1_phylumKey, CAST(t1.classKey AS String) AS t1_classKey, CAST(t1.orderKey AS String) AS t1_orderKey, CAST(t1.familyKey AS String) AS t1_familyKey, CAST(t1.genusKey AS String) AS t1_genusKey, CAST(t1.speciesKey AS String) AS t1_speciesKey, CAST(t1.acceptedTaxonKey AS String) AS t1_acceptedTaxonKey, CAST(t1.taxonKey AS String) AS t1_taxonKey,
        t1.scientificName AS t1_scientificName, t1.acceptedScientificName AS t1_acceptedScientificName, t1.kingdom AS t1_kingdom, t1.phylum AS t1_phylum, t1.order_ AS t1_order, t1.family AS t1_family, t1.genus AS t1_genus, t1.species AS t1_species, t1.genericName AS t1_genericName, t1.specificEpithet AS t1_specificEpithet, t1.taxonRank AS t1_taxonRank,
        t1.typeStatus AS t1_typeStatus, t1.preparations AS t1_preparations,
        t1.decimalLatitude AS t1_decimalLatitude, t1.decimalLongitude AS t1_decimalLongitude, t1.countryCode AS t1_countryCode,
        t1.year AS t1_year, t1.month AS t1_month, t1.day AS t1_day, from_unixtime(floor(t1.eventDate/1000)) AS t1_eventDate,
        t1.recordNumber AS t1_recordNumber, t1.fieldNumber AS t1_fieldNumber, t1.occurrenceID AS t1_occurrenceID, t1.otherCatalogNumbers AS t1_otherCatalogNumbers, t1.institutionCode AS t1_institutionCode, t1.collectionCode AS t1_collectionCode, t1.catalogNumber AS t1_catalogNumber,
        t1.recordedBy AS t1_recordedBy, t1.recordedByID AS t1_recordedByID,
        t1.ext_multimedia AS t1_media,

        t2.gbifId AS t2_gbifId, t2.datasetKey AS t2_datasetKey, t2.basisOfRecord AS t2_basisOfRecord, t2.publishingorgkey AS t2_publishingOrgKey, t2.datasetName AS t2_datasetName, t2.publisher AS t2_publishingOrgName,
        CAST(t2.kingdomKey AS String) AS t2_kingdomKey, CAST(t2.phylumKey AS String) AS t2_phylumKey, CAST(t2.classKey AS String) AS t2_classKey, CAST(t2.orderKey AS String) AS t2_orderKey, CAST(t2.familyKey AS String) AS t2_familyKey, CAST(t2.genusKey AS String) AS t2_genusKey, CAST(t2.speciesKey AS String) AS t2_speciesKey, CAST(t2.acceptedTaxonKey AS String) AS t2_acceptedTaxonKey, CAST(t2.taxonKey AS String) AS t2_taxonKey,
        t2.scientificName AS t2_scientificName, t2.acceptedScientificName AS t2_acceptedScientificName, t2.kingdom AS t2_kingdom, t2.phylum AS t2_phylum, t2.order_ AS t2_order, t2.family AS t2_family, t2.genus AS t2_genus, t2.species AS t2_species, t2.genericName AS t2_genericName, t2.specificEpithet AS t2_specificEpithet, t2.taxonRank AS t2_taxonRank,
        t2.typeStatus AS t2_typeStatus, t2.preparations AS t2_preparations,
        t2.decimalLatitude AS t2_decimalLatitude, t2.decimalLongitude AS t2_decimalLongitude, t2.countryCode AS t2_countryCode,
        t2.year AS t2_year, t2.month AS t2_month, t2.day AS t2_day, from_unixtime(floor(t2.eventDate/1000)) AS t2_eventDate,
        t2.recordNumber AS t2_recordNumber, t2.fieldNumber AS t2_fieldNumber, t2.occurrenceID AS t2_occurrenceID, t2.otherCatalogNumbers AS t2_otherCatalogNumbers, t2.institutionCode AS t2_institutionCode, t2.collectionCode AS t2_collectionCode, t2.catalogNumber AS t2_catalogNumber,
        t2.recordedBy AS t2_recordedBy, t2.recordedByID AS t2_recordedByID,
        t2.ext_multimedia AS t2_media

      FROM """ + hiveTableCandidates + """ h
       JOIN occurrence t1 ON h.id1 = t1.gbifID
       JOIN occurrence t2 ON h.id2 = t2.gbifID
      """);

    import org.apache.spark.sql.types._
    // schema holds redundant information, but aids diagnostics in Hive at low cost
    val relationshipSchema = StructType(
      StructField("id1", StringType, nullable = false) ::
        StructField("id2", StringType, nullable = false) ::
        StructField("reasons", StringType, nullable = false) ::
        StructField("dataset1", StringType, nullable = false) ::
        StructField("dataset2", StringType, nullable = false) ::
        StructField("o1", StringType, nullable = false) ::
        StructField("o2", StringType, nullable = false) :: Nil

    )
    val relationshipEncoder = RowEncoder(relationshipSchema)

    val relationships = pairs.flatMap(row => {
      val records = scala.collection.mutable.ListBuffer[Row]()

      val o1 = new RowOccurrenceFeatures(row, "t1_", "t1_media")
      val o2 = new RowOccurrenceFeatures(row, "t2_", "t2_media")

      val relationships: Option[RelationshipAssertion[RowOccurrenceFeatures]] = Option(OccurrenceRelationships.generate(o1,o2))
      relationships match {
        case Some(r) => {
          // store both ways
          records.append(Row(
            String.valueOf(r.getOcc1.getLong("gbifId")),
            String.valueOf(r.getOcc2.getLong("gbifId")),
            r.getJustificationAsDelimited,
            r.getOcc1.get("datasetKey"),
            r.getOcc2.get("datasetKey"),
            r.getOcc1.asJson(),
            r.getOcc2.asJson()))

          records.append(Row(
            String.valueOf(r.getOcc2.getLong("gbifId")),
            String.valueOf(r.getOcc1.getLong("gbifId")),
            r.getJustificationAsDelimited,
            r.getOcc2.get("datasetKey"),
            r.getOcc1.get("datasetKey"),
            r.getOcc2.asJson(),
            r.getOcc1.asJson()))
        }
        case None => // skip
      }

      records

    })(relationshipEncoder).toDF().dropDuplicates()

    relationships.write.saveAsTable(hiveTableRelationships) // for diagnostics in hive

    // convert to HBase, with modulo salted keys
    val relationshipsSorted = relationships.rdd.flatMap(r => {
      // index based access as cannot access by schema using flatMap and rdd
      val id1 = r.getString(0)
      val id2 = r.getString(1)
      val relationshipType = r.getString(2)
      val dataset1 = r.getString(3)
      val dataset2 = r.getString(4)
      val occurrence1 = r.getString(5)
      val occurrence2 = r.getString(6)

      // we salt in HBase only on the id1 to enable prefix scanning using an occurrence ID
      val salt = Math.abs(id1.hashCode) % hbaseRegions

      val saltedRowKey = salt + ":" + id1 + ":" + id2
      val cells = scala.collection.mutable.ListBuffer[((String, String), String)]()

      // while only occurrence2 is needed it is not expensive to store each which aids diagnostics
      cells.append(((saltedRowKey, "id1"),id1))
      cells.append(((saltedRowKey, "id2"),id2))
      cells.append(((saltedRowKey, "reasons"),relationshipType))
      cells.append(((saltedRowKey, "dataset1"),dataset1))
      cells.append(((saltedRowKey, "dataset2"),dataset2))
      cells.append(((saltedRowKey, "occurrence1"),occurrence1))
      cells.append(((saltedRowKey, "occurrence2"),occurrence2))

      cells
    }).repartitionAndSortWithinPartitions(new SaltPartitioner(hbaseRegions)).map(cell => {
      val k = new ImmutableBytesWritable(Bytes.toBytes(cell._1._1))
      val row = new KeyValue(Bytes.toBytes(cell._1._1), // key
        Bytes.toBytes("o"), // column family
        Bytes.toBytes(cell._1._2), // cell
        Bytes.toBytes(cell._2) // cell value
      )

      (k, row)
    })

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", hbaseZK);
    // NOTE: job creates a copy of the conf
    val job = new Job(conf,"Relationships") // name not actually used since we don't submit MR
    job.setJarByClass(this.getClass)
    val table = new HTable(conf, hbaseTable)
    HFileOutputFormat2.configureIncrementalLoad(job, table);
    val conf2 = job.getConfiguration // important

    relationshipsSorted.saveAsNewAPIHadoopFile(hfileDir, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat], conf2)
  }

  /**
   * Sanitizes application arguments.
   */
  private def checkArgs(args: Array[String]) : Map[Symbol, String] = {
    assert(args != null && args.length==16, usage)

    def nextOption(map : Map[Symbol, String], list: List[String]) : Map[Symbol, String] = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--hive-db" :: value :: tail =>
          nextOption(map ++ Map('hiveDatabase -> value), tail)
        case "--hive-table-hashed" :: value :: tail =>
          nextOption(map ++ Map('hiveTableHashed -> value), tail)
        case "--hive-table-candidates" :: value :: tail =>
          nextOption(map ++ Map('hiveTableCandidates -> value), tail)
        case "--hive-table-relationships" :: value :: tail =>
          nextOption(map ++ Map('hiveTableRelationships -> value), tail)
        case "--hbase-table" :: value :: tail =>
          nextOption(map ++ Map('hbaseTable -> value), tail)
        case "--hbase-regions" :: value :: tail =>
          nextOption(map ++ Map('hbaseRegions -> value), tail)
        case "--hbase-zk" :: value :: tail =>
          nextOption(map ++ Map('hbaseZK -> value), tail)
        case "--hfile-dir" :: value :: tail =>
          nextOption(map ++ Map('hfileDir -> value), tail)
        case option :: tail => println("Unknown option "+option)
          System.exit(1)
          map
      }
    }
    nextOption(Map(), args.toList)
  }
}
