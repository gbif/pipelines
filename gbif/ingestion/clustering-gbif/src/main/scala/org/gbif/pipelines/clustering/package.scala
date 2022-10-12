package org.gbif.pipelines

import org.apache.spark.sql.Row

package object clustering {
  // IDs to skip
  val omitIds = List("NO APLICA", "NA", "[]", "NO DISPONIBLE", "NO DISPONIBL", "NO NUMBER", "--", "UNKNOWN")

  // SPECIMENS
  val specimenBORs = List("PRESERVED_SPECIMEN", "MATERIAL_SAMPLE", "LIVING_SPECIMEN", "FOSSIL_SPECIMEN", "MATERIAL_CITATION")

  // SQL to extract fields necessary for grouping for candidate pairs
  val SQL_OCCURRENCE = """
SELECT
  gbifId, datasetKey, basisOfRecord, publishingorgkey, datasetName, publisher,
  kingdomKey, phylumKey, classKey, orderKey, familyKey, genusKey, speciesKey, acceptedTaxonKey, taxonKey,
  scientificName, acceptedScientificName, kingdom, phylum, order_ AS order, family, genus, species, genericName, specificEpithet, taxonRank,
  typeStatus, preparations,
  decimalLatitude, decimalLongitude, countryCode,
  year, month, day, from_unixtime(floor(eventDate/1000)) AS eventDate,
  recordNumber, fieldNumber, occurrenceID, otherCatalogNumbers, institutionCode, collectionCode, catalogNumber,
  recordedBy, recordedByID,
  ext_multimedia
FROM occurrence
WHERE speciesKey IS NOT NULL
"""

  case class SimpleOccurrence(gbifID: String, decimalLatitude: Double)

  /**
   * @return A triplified version of the codes if all present, otherwise None
   */
  def triplify(r: Row) : Option[String] = {
    val ic = Option(r.getAs[String]("institutionCode"));
    val cc = Option(r.getAs[String]("collectionCode"));
    val cn = Option(r.getAs[String]("catalogNumber"));

    if (!ic.isEmpty && !cc.isEmpty && !cn.isEmpty) Option(ic.get + ":" + cc.get + ":" + cn.get)
    else None
  }

  /**
    * @return The catalogNumber prefixed by the institutionCode if both present, otherwise None
    */
  def scopeCatalogNumber(r: Row) : Option[String] = {
    // This format of catalog number used by e.g. ENA datasets
    val ic = Option(r.getAs[String]("institutionCode"));
    val cn = Option(r.getAs[String]("catalogNumber"));

    if (!ic.isEmpty && !cn.isEmpty) Option(ic.get + ":" + cn.get)
    else None
  }
}
