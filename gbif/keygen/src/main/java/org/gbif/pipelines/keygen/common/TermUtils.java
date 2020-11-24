package org.gbif.pipelines.keygen.common;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

/**
 * Static utils class to deal with Term enumeration for occurrences. Note to developers: If you
 * modify this class, make sure to have a look at org.gbif.occurrence.download.hive.Terms in the
 * occurrence-hdfs-table module.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TermUtils {

  /** Interpreted terms that exist as java properties on Occurrence. */
  private static final ImmutableSet<Term> JAVA_PROPERTY_TERMS =
      ImmutableSet.of(
          DwcTerm.decimalLatitude,
          DwcTerm.decimalLongitude,
          DwcTerm.continent,
          DwcTerm.waterBody,
          DwcTerm.stateProvince,
          DwcTerm.countryCode,
          DwcTerm.dateIdentified,
          DwcTerm.eventDate,
          DwcTerm.year,
          DwcTerm.month,
          DwcTerm.day,
          DwcTerm.kingdom,
          DwcTerm.phylum,
          DwcTerm.class_,
          DwcTerm.order,
          DwcTerm.family,
          DwcTerm.genus,
          DwcTerm.subgenus,
          GbifTerm.species,
          DwcTerm.scientificName,
          DwcTerm.taxonRank,
          DwcTerm.taxonomicStatus,
          GbifTerm.acceptedScientificName,
          GbifTerm.genericName,
          DwcTerm.specificEpithet,
          DwcTerm.infraspecificEpithet,
          DwcTerm.basisOfRecord,
          DwcTerm.individualCount,
          DwcTerm.sex,
          DwcTerm.lifeStage,
          DwcTerm.establishmentMeans,
          GbifTerm.taxonKey,
          GbifTerm.acceptedTaxonKey,
          DwcTerm.typeStatus,
          GbifTerm.typifiedName,
          GbifTerm.kingdomKey,
          GbifTerm.phylumKey,
          GbifTerm.classKey,
          GbifTerm.orderKey,
          GbifTerm.familyKey,
          GbifTerm.genusKey,
          GbifTerm.subgenusKey,
          GbifTerm.speciesKey,
          GbifTerm.datasetKey,
          GbifTerm.publishingCountry,
          GbifTerm.lastInterpreted,
          DcTerm.modified,
          DwcTerm.coordinateUncertaintyInMeters,
          DwcTerm.coordinatePrecision,
          GbifTerm.elevation,
          GbifTerm.elevationAccuracy,
          GbifTerm.depth,
          GbifTerm.depthAccuracy,
          GbifInternalTerm.unitQualifier,
          GbifTerm.issue,
          DcTerm.references,
          GbifTerm.datasetKey,
          GbifTerm.publishingCountry,
          GbifTerm.protocol,
          GbifTerm.lastCrawled,
          GbifTerm.lastParsed,
          DcTerm.license);

  /**
   * TODO: is this correct? -> Terms used during interpretation and superseded by an interpreted
   * property
   */
  private static final ImmutableSet<Term> INTERPRETED_SOURCE_TERMS =
      ImmutableSet.copyOf(
          Iterables.concat(
              JAVA_PROPERTY_TERMS,
              Lists.newArrayList(
                  DwcTerm.decimalLatitude,
                  DwcTerm.decimalLongitude,
                  DwcTerm.verbatimLatitude,
                  DwcTerm.verbatimLongitude,
                  DwcTerm.verbatimCoordinates,
                  DwcTerm.geodeticDatum,
                  DwcTerm.coordinateUncertaintyInMeters,
                  DwcTerm.coordinatePrecision,
                  DwcTerm.continent,
                  DwcTerm.waterBody,
                  DwcTerm.stateProvince,
                  DwcTerm.country,
                  DwcTerm.countryCode,
                  DwcTerm.scientificName,
                  DwcTerm.scientificNameAuthorship,
                  DwcTerm.taxonRank,
                  DwcTerm.taxonomicStatus,
                  DwcTerm.kingdom,
                  DwcTerm.phylum,
                  DwcTerm.class_,
                  DwcTerm.order,
                  DwcTerm.family,
                  DwcTerm.genus,
                  DwcTerm.subgenus,
                  GbifTerm.genericName,
                  DwcTerm.specificEpithet,
                  DwcTerm.infraspecificEpithet,
                  DcTerm.modified,
                  DwcTerm.dateIdentified,
                  DwcTerm.eventDate,
                  DwcTerm.year,
                  DwcTerm.month,
                  DwcTerm.day,
                  DwcTerm.minimumDepthInMeters,
                  DwcTerm.maximumDepthInMeters,
                  DwcTerm.minimumElevationInMeters,
                  DwcTerm.maximumElevationInMeters,
                  DwcTerm.associatedMedia)));

  /**
   * @return true if the term is used during interpretation and superseded by an interpreted
   *     property
   */
  public static boolean isInterpretedSourceTerm(Term term) {
    return INTERPRETED_SOURCE_TERMS.contains(term);
  }

  /** @return true if the term is an interpreted value stored as a java property on Occurrence. */
  public static boolean isOccurrenceJavaProperty(Term term) {
    return JAVA_PROPERTY_TERMS.contains(term);
  }
}
