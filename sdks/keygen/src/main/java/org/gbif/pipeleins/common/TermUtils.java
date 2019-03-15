package org.gbif.pipeleins.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Static utils class to deal with Term enumeration for occurrences.
 * Note to developers:
 * If you modify this class, make sure to have a look at org.gbif.occurrence.download.hive.Terms  in the
 * occurrence-hdfs-table module.
 *
 */
public class TermUtils {

  private static final Set<? extends Term> EXTENSION_TERMS = Arrays.stream(Extension.values())
    .map(ext -> TermFactory.instance().findTerm(ext.getRowType())).collect(Collectors.toSet());


  private static final Set<? extends Term> INTERPRETED_DATES = ImmutableSet.of(DwcTerm.eventDate,
                                                                               DwcTerm.dateIdentified,
                                                                               GbifTerm.lastInterpreted,
                                                                               GbifTerm.lastParsed,
                                                                               GbifTerm.lastCrawled,
                                                                               DcTerm.modified,
                                                                               GbifInternalTerm.fragmentCreated);

  private static final Set<? extends Term> INTERPRETED_NUM = ImmutableSet.of(DwcTerm.year,
                                                                             DwcTerm.month,
                                                                             DwcTerm.day,
                                                                             DwcTerm.individualCount,
                                                                             GbifTerm.taxonKey,
                                                                             GbifTerm.kingdomKey,
                                                                             GbifTerm.phylumKey,
                                                                             GbifTerm.classKey,
                                                                             GbifTerm.orderKey,
                                                                             GbifTerm.familyKey,
                                                                             GbifTerm.genusKey,
                                                                             GbifTerm.subgenusKey,
                                                                             GbifTerm.speciesKey,
                                                                             GbifTerm.acceptedTaxonKey,
                                                                             GbifInternalTerm.crawlId,
                                                                             GbifInternalTerm.identifierCount);

  private static final Set<? extends Term> INTERPRETED_BOOLEAN =
    ImmutableSet.of(GbifTerm.hasCoordinate, GbifTerm.hasGeospatialIssues);

  private static final Set<? extends Term> COMPLEX_TYPE = ImmutableSet.of(GbifTerm.mediaType, GbifTerm.issue);

  private static final Set<? extends Term> INTERPRETED_DOUBLE = ImmutableSet.of(DwcTerm.decimalLatitude,
                                                                                DwcTerm.decimalLongitude,
                                                                                GbifTerm.coordinateAccuracy,
                                                                                GbifTerm.elevation,
                                                                                GbifTerm.elevationAccuracy,
                                                                                GbifTerm.depth,
                                                                                GbifTerm.depthAccuracy,
                                                                                DwcTerm.coordinateUncertaintyInMeters,
                                                                                DwcTerm.coordinatePrecision);

  private static final Set<? extends Term> NON_OCCURRENCE_TERMS =
    ImmutableSet.copyOf(Iterables.concat(DwcTerm.listByGroup(DwcTerm.GROUP_MEASUREMENTORFACT),
                                                               DwcTerm.listByGroup(DwcTerm.GROUP_RESOURCERELATIONSHIP),
                                                               Sets.newHashSet(GbifTerm.infraspecificMarker,
                                                                               GbifTerm.isExtinct,
                                                                               GbifTerm.isFreshwater,
                                                                               GbifTerm.isHybrid,
                                                                               GbifTerm.isMarine,
                                                                               GbifTerm.isPlural,
                                                                               GbifTerm.isPreferredName,
                                                                               GbifTerm.isTerrestrial,
                                                                               GbifTerm.livingPeriod,
                                                                               GbifTerm.lifeForm,
                                                                               GbifTerm.ageInDays,
                                                                               GbifTerm.sizeInMillimeter,
                                                                               GbifTerm.massInGram,
                                                                               GbifTerm.organismPart,
                                                                               GbifTerm.appendixCITES,
                                                                               GbifTerm.typeDesignatedBy,
                                                                               GbifTerm.typeDesignationType,
                                                                               GbifTerm.canonicalName,
                                                                               GbifTerm.nameType,
                                                                               GbifTerm.verbatimLabel,
                                                                               GbifTerm.infraspecificMarker,
                                                                               GbifTerm.numOfOccurrences)));

  /**
   * Interpreted terms that exist as java properties on Occurrence.
   */
  private static final Set<? extends Term> JAVA_PROPERTY_TERMS = ImmutableSet.of(DwcTerm.decimalLatitude,
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
   * TODO: is this correct? -> Terms used during interpretation and superseded by an interpreted property
   */
  private static final Set<? extends Term> INTERPRETED_SOURCE_TERMS =
                          ImmutableSet.copyOf(Iterables.concat(JAVA_PROPERTY_TERMS,
                                                               Lists.newArrayList(DwcTerm.decimalLatitude,
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
   * Term list of the extension excluding the coreid just as defined by:
   * http://rs.gbif.org/extension/gbif/1.0/multimedia.xml
   */
  private static final List<DcTerm> MULTIMEDIA_TERMS = ImmutableList.of(DcTerm.type,
                                                                        DcTerm.format,
                                                                        DcTerm.identifier,
                                                                        DcTerm.references,
                                                                        DcTerm.title,
                                                                        DcTerm.description,
                                                                        DcTerm.created,
                                                                        DcTerm.creator,
                                                                        DcTerm.contributor,
                                                                        DcTerm.publisher,
                                                                        DcTerm.audience,
                                                                        DcTerm.source,
                                                                        DcTerm.license,
                                                                        DcTerm.rightsHolder);

  private TermUtils() {
    // private constructor
  }


  /**
   * Lists all terms that have been used during interpretation and are superseded by an interpreted,
   * typed java Occurrence property.
   *
   * @return iterable of terms that have been used during interpretation
   */
  public static Iterable<? extends Term> interpretedSourceTerms() {
    return INTERPRETED_SOURCE_TERMS;
  }

  /**
   * @return true if the term is used during interpretation and superseded by an interpreted property
   */
  public static boolean isInterpretedSourceTerm(Term term) {
    return INTERPRETED_SOURCE_TERMS.contains(term);
  }

  /**
   * @return true if the term is an interpreted value stored as a java property on Occurrence.
   */
  public static boolean isOccurrenceJavaProperty(Term term) {
    return JAVA_PROPERTY_TERMS.contains(term);
  }

  /**
   * Lists all terms relevant for an interpreted occurrence record, starting with occurrenceID as the key.
   * UnknownTerms are not included as they are open ended.
   */
  public static Iterable<? extends Term> interpretedTerms() {
    return Iterables.concat(Lists.newArrayList(GbifTerm.gbifID),
      Arrays.stream(DcTerm.values()).filter(t -> !t.isClass() && (!INTERPRETED_SOURCE_TERMS.contains(t)
                                                 || JAVA_PROPERTY_TERMS.contains(t))).collect(Collectors.toList()),
      Arrays.stream(DwcTerm.values()).filter(t -> !t.isClass() && !NON_OCCURRENCE_TERMS.contains(t)
                                                  && (!INTERPRETED_SOURCE_TERMS.contains(t)
            || JAVA_PROPERTY_TERMS.contains(t))).collect(Collectors.toList()),
      // GbifTerm.coordinateAccuracy is deprecated
      Arrays.stream(GbifTerm.values()).filter(t -> !t.isClass() && !NON_OCCURRENCE_TERMS.contains(t)
                                                    && GbifTerm.gbifID != t && GbifTerm.coordinateAccuracy !=t).collect(Collectors.toList()));
  }

  /**
   * Lists all terms relevant for a verbatim occurrence record.
   * gbifID is included and comes first as its the foreign key to the core record.
   * UnknownTerms are not included as they are open ended.
   */
  public static Iterable<? extends Term> verbatimTerms() {
    return Iterables.concat(Collections.singletonList(GbifTerm.gbifID),
                         Arrays.stream(DcTerm.values()).filter(t -> !t.isClass()).collect(Collectors.toList()),
                         Arrays.stream(DwcTerm.values())
                           .filter(t -> !t.isClass() && !NON_OCCURRENCE_TERMS.contains(t)).collect(Collectors.toList()));
  }

  /**
   * Lists all terms relevant for a multimedia extension record.
   * gbifID is included and comes first as its the foreign key to the core record.
   */
  public static Iterable<Term> multimediaTerms() {
    return Iterables.concat(Collections.singletonList(GbifTerm.gbifID), MULTIMEDIA_TERMS);
  }

  /**
   * @return true if the term is an interpreted date and stored as a binary in HBase
   */
  public static boolean isInterpretedDate(Term term) {
    return INTERPRETED_DATES.contains(term);
  }

  /**
   * @return true if the term is an interpreted numerical and stored as a binary in HBase
   */
  public static boolean isInterpretedNumerical(Term term) {
    return INTERPRETED_NUM.contains(term);
  }

  /**
   * @return true if the term is an interpreted double and stored as a binary in HBase
   */
  public static boolean isInterpretedDouble(Term term) {
    return INTERPRETED_DOUBLE.contains(term);
  }

  /**
   * @return true if the term is an interpreted boolean and stored as a binary in HBase
   */
  public static boolean isInterpretedBoolean(Term term) {
    return INTERPRETED_BOOLEAN.contains(term);
  }

  /**
   * @return true if the term is an complex type in Hive or Hbase: array, struct, json, etc.
   */
  public static boolean isComplexType(Term term) {
    return COMPLEX_TYPE.contains(term);
  }

  public static boolean isExtensionTerm(Term term) {
    return EXTENSION_TERMS.contains(term);
  }

}
