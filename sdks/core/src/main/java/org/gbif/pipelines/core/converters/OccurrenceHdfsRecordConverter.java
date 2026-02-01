package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.utils.ModelUtils.extractOptValue;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.pojo.HumboldtJsonView;
import org.gbif.pipelines.core.utils.MediaSerDeser;
import org.gbif.pipelines.core.utils.TemporalConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.terms.utils.TermUtils;

/** Utility class to convert interpreted and extended records into {@link OccurrenceHdfsRecord}. */
@Slf4j
@Builder
public class OccurrenceHdfsRecordConverter {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private final ExtendedRecord extendedRecord;
  private final IdentifierRecord identifierRecord;
  private final ClusteringRecord clusteringRecord;
  private final BasicRecord basicRecord;
  private final LocationRecord locationRecord;
  private final MultiTaxonRecord multiTaxonRecord;
  private final GrscicollRecord grscicollRecord;
  private final TemporalRecord temporalRecord;
  private final MetadataRecord metadataRecord;
  private final MultimediaRecord multimediaRecord;
  private final DnaDerivedDataRecord dnaDerivedDataRecord;
  private final EventCoreRecord eventCoreRecord;
  private final HumboldtRecord humboldtRecord;

  // required taxonomic terms. These need to be populated at least with empty string
  // to support SQL queries and avoid 'Key not found' errors.
  static final Set<String> REQUIRED_TAXONOMIC_FIELDS =
      Set.of(
          GbifTerm.taxonKey.simpleName().toLowerCase(),
          DwcTerm.scientificName.simpleName().toLowerCase(),
          GbifTerm.acceptedTaxonKey.simpleName().toLowerCase(),
          DwcTerm.acceptedNameUsageID.simpleName().toLowerCase(),
          GbifTerm.acceptedScientificName.simpleName().toLowerCase(),
          DwcTerm.genericName.simpleName().toLowerCase(),
          DwcTerm.specificEpithet.simpleName().toLowerCase(),
          DwcTerm.infraspecificEpithet.simpleName().toLowerCase(),
          DwcTerm.taxonRank.simpleName().toLowerCase(),
          DwcTerm.class_.simpleName().toLowerCase(),
          GbifTerm.classKey.simpleName().toLowerCase(),
          DwcTerm.family.simpleName().toLowerCase(),
          GbifTerm.familyKey.simpleName().toLowerCase(),
          DwcTerm.genus.simpleName().toLowerCase(),
          GbifTerm.genusKey.simpleName().toLowerCase(),
          DwcTerm.kingdom.simpleName().toLowerCase(),
          GbifTerm.kingdomKey.simpleName().toLowerCase(),
          DwcTerm.order.simpleName().toLowerCase(),
          GbifTerm.orderKey.simpleName().toLowerCase(),
          DwcTerm.phylum.simpleName().toLowerCase(),
          GbifTerm.phylumKey.simpleName().toLowerCase(),
          GbifTerm.species.simpleName().toLowerCase(),
          DwcTerm.subfamily.simpleName().toLowerCase(),
          GbifTerm.subfamilyKey.simpleName().toLowerCase(),
          DwcTerm.superfamily.simpleName().toLowerCase(),
          DwcTerm.tribe.simpleName().toLowerCase(),
          GbifTerm.tribeKey.simpleName().toLowerCase(),
          DwcTerm.subtribe.simpleName().toLowerCase(),
          GbifTerm.subtribeKey.simpleName().toLowerCase(),
          IucnTerm.iucnRedListCategory.simpleName().toLowerCase(),
          GbifTerm.verbatimScientificName.simpleName().toLowerCase());

  /**
   * Collects data from {@link SpecificRecordBase} instances into a {@link OccurrenceHdfsRecord}.
   *
   * @return a {@link OccurrenceHdfsRecord} instance based on the input records
   */
  public OccurrenceHdfsRecord convert() {
    OccurrenceHdfsRecord occurrenceHdfsRecord = new OccurrenceHdfsRecord();
    occurrenceHdfsRecord.setIssue(new ArrayList<>());
    occurrenceHdfsRecord.setNontaxonomicissue(new ArrayList<>());

    // Order is important
    mapIdentifierRecord(occurrenceHdfsRecord);
    mapClusteringRecord(occurrenceHdfsRecord);
    mapBasicRecord(occurrenceHdfsRecord);
    mapMetadataRecord(occurrenceHdfsRecord);
    mapTemporalRecord(occurrenceHdfsRecord);
    mapLocationRecord(occurrenceHdfsRecord);
    mapMultiTaxonRecord(occurrenceHdfsRecord);
    mapGrscicollRecord(occurrenceHdfsRecord);
    mapMultimediaRecord(occurrenceHdfsRecord);
    mapExtendedRecord(occurrenceHdfsRecord);
    mapEventCoreRecord(occurrenceHdfsRecord);
    mapProjectIds(occurrenceHdfsRecord);
    mapDnaDerivedDataRecord(occurrenceHdfsRecord);
    mapHumboldtRecord(occurrenceHdfsRecord);

    return occurrenceHdfsRecord;
  }

  /**
   * Sets the lastInterpreted and lastParsed dates if the new value is greater that the existing one
   * or if it is not set.
   */
  private static void setCreatedIfGreater(OccurrenceHdfsRecord occurrenceHdfsRecord, Long created) {
    if (Objects.nonNull(created)) {
      Long maxCreated =
          Math.max(
              created,
              Optional.ofNullable(occurrenceHdfsRecord.getLastinterpreted())
                  .orElse(Long.MIN_VALUE));
      occurrenceHdfsRecord.setLastinterpreted(maxCreated);
      occurrenceHdfsRecord.setLastparsed(maxCreated);
    }
  }

  /**
   * Adds the list of issues to the list of issues in the {@link OccurrenceHdfsRecord}.
   *
   * @param issueRecord record issues
   * @param occurrenceHdfsRecord target object
   */
  private static void addNonTaxonIssues(
      IssueRecord issueRecord, OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (issueRecord == null || issueRecord.getIssueList() == null) {
      return;
    }

    List<String> issues = issueRecord.getIssueList();

    occurrenceHdfsRecord.getIssue().addAll(issues);
    occurrenceHdfsRecord.getNontaxonomicissue().addAll(issues);
  }

  /**
   * Adds the list of issues to the list of issues in the {@link OccurrenceHdfsRecord}.
   *
   * @param issueRecord record issues
   * @param occurrenceHdfsRecord target object
   */
  private static void addTaxonIssues(
      IssueRecord issueRecord, OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (Objects.nonNull(issueRecord) && Objects.nonNull(issueRecord.getIssueList())) {
      List<String> currentIssues = occurrenceHdfsRecord.getIssue();
      currentIssues.addAll(issueRecord.getIssueList());
      occurrenceHdfsRecord.setIssue(currentIssues);
    }
  }

  /** Copies the {@link LocationRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapLocationRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (locationRecord == null) {
      return;
    }
    occurrenceHdfsRecord.setCountrycode(locationRecord.getCountryCode());
    occurrenceHdfsRecord.setContinent(locationRecord.getContinent());
    occurrenceHdfsRecord.setDecimallatitude(locationRecord.getDecimalLatitude());
    occurrenceHdfsRecord.setDecimallongitude(locationRecord.getDecimalLongitude());
    occurrenceHdfsRecord.setCoordinateprecision(locationRecord.getCoordinatePrecision());
    occurrenceHdfsRecord.setCoordinateuncertaintyinmeters(
        locationRecord.getCoordinateUncertaintyInMeters());
    occurrenceHdfsRecord.setDepth(locationRecord.getDepth());
    occurrenceHdfsRecord.setDepthaccuracy(locationRecord.getDepthAccuracy());
    occurrenceHdfsRecord.setElevation(locationRecord.getElevation());
    occurrenceHdfsRecord.setElevationaccuracy(locationRecord.getElevationAccuracy());
    if (Objects.nonNull(locationRecord.getMaximumDistanceAboveSurfaceInMeters())) {
      occurrenceHdfsRecord.setMaximumdistanceabovesurfaceinmeters(
          locationRecord.getMaximumDistanceAboveSurfaceInMeters().toString());
    }
    if (Objects.nonNull(locationRecord.getMinimumDistanceAboveSurfaceInMeters())) {
      occurrenceHdfsRecord.setMinimumdistanceabovesurfaceinmeters(
          locationRecord.getMinimumDistanceAboveSurfaceInMeters().toString());
    }
    occurrenceHdfsRecord.setStateprovince(locationRecord.getStateProvince());
    occurrenceHdfsRecord.setWaterbody(locationRecord.getWaterBody());
    occurrenceHdfsRecord.setHascoordinate(locationRecord.getHasCoordinate());
    occurrenceHdfsRecord.setHasgeospatialissues(locationRecord.getHasGeospatialIssue());
    occurrenceHdfsRecord.setRepatriated(locationRecord.getRepatriated());
    occurrenceHdfsRecord.setLocality(locationRecord.getLocality());
    occurrenceHdfsRecord.setPublishingcountry(locationRecord.getPublishingCountry());
    Optional.ofNullable(locationRecord.getGadm())
        .ifPresent(
            g -> {
              occurrenceHdfsRecord.setLevel0gid(g.getLevel0Gid());
              occurrenceHdfsRecord.setLevel1gid(g.getLevel1Gid());
              occurrenceHdfsRecord.setLevel2gid(g.getLevel2Gid());
              occurrenceHdfsRecord.setLevel3gid(g.getLevel3Gid());
              occurrenceHdfsRecord.setLevel0name(g.getLevel0Name());
              occurrenceHdfsRecord.setLevel1name(g.getLevel1Name());
              occurrenceHdfsRecord.setLevel2name(g.getLevel2Name());
              occurrenceHdfsRecord.setLevel3name(g.getLevel3Name());
            });
    occurrenceHdfsRecord.setDistancefromcentroidinmeters(
        locationRecord.getDistanceFromCentroidInMeters());
    occurrenceHdfsRecord.setGbifregion(locationRecord.getGbifRegion());
    occurrenceHdfsRecord.setPublishedbygbifregion(locationRecord.getPublishedByGbifRegion());
    occurrenceHdfsRecord.setHighergeography(locationRecord.getHigherGeography());
    occurrenceHdfsRecord.setGeoreferencedby(locationRecord.getGeoreferencedBy());

    setCreatedIfGreater(occurrenceHdfsRecord, locationRecord.getCreated());
    addNonTaxonIssues(locationRecord.getIssues(), occurrenceHdfsRecord);
  }

  private void mapProjectIds(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    Set<String> projectIds = new HashSet<>();

    if (metadataRecord != null) {
      projectIds.add(metadataRecord.getProjectId());
    }

    if (basicRecord != null) {
      projectIds.addAll(basicRecord.getProjectId());
    }

    if (!projectIds.isEmpty()) {
      occurrenceHdfsRecord.setProjectid(new ArrayList<>(projectIds));
    }
  }

  /** Copies the {@link MetadataRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapMetadataRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (metadataRecord == null) {
      return;
    }
    occurrenceHdfsRecord.setCrawlid(metadataRecord.getCrawlId());
    occurrenceHdfsRecord.setDatasetkey(metadataRecord.getDatasetKey());
    occurrenceHdfsRecord.setDatasettitle(metadataRecord.getDatasetTitle());
    occurrenceHdfsRecord.setInstallationkey(metadataRecord.getInstallationKey());
    occurrenceHdfsRecord.setProtocol(metadataRecord.getProtocol());
    occurrenceHdfsRecord.setNetworkkey(metadataRecord.getNetworkKeys());
    occurrenceHdfsRecord.setPublisher(metadataRecord.getPublisherTitle());
    occurrenceHdfsRecord.setPublishingorgkey(metadataRecord.getPublishingOrganizationKey());
    occurrenceHdfsRecord.setLastcrawled(metadataRecord.getLastCrawled());
    occurrenceHdfsRecord.setProgrammeacronym(metadataRecord.getProgrammeAcronym());
    occurrenceHdfsRecord.setHostingorganizationkey(metadataRecord.getHostingOrganizationKey());

    if (occurrenceHdfsRecord.getLicense() == null) {
      occurrenceHdfsRecord.setLicense(metadataRecord.getLicense());
    }

    setCreatedIfGreater(occurrenceHdfsRecord, metadataRecord.getCreated());
    addNonTaxonIssues(metadataRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link TemporalRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapTemporalRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (temporalRecord == null) {
      return;
    }
    Optional.ofNullable(temporalRecord.getDateIdentified())
        .map(StringToDateFunctions.getStringToEarliestEpochSeconds(false))
        .ifPresent(occurrenceHdfsRecord::setDateidentified);
    Optional.ofNullable(temporalRecord.getModified())
        .map(StringToDateFunctions.getStringToEarliestEpochSeconds(false))
        .ifPresent(occurrenceHdfsRecord::setModified);
    occurrenceHdfsRecord.setDay(temporalRecord.getDay());
    occurrenceHdfsRecord.setMonth(temporalRecord.getMonth());
    occurrenceHdfsRecord.setYear(temporalRecord.getYear());

    if (Objects.nonNull(temporalRecord.getStartDayOfYear())) {
      occurrenceHdfsRecord.setStartdayofyear(temporalRecord.getStartDayOfYear());
    } else {
      occurrenceHdfsRecord.setStartdayofyear(null);
    }

    if (Objects.nonNull(temporalRecord.getEndDayOfYear())) {
      occurrenceHdfsRecord.setEnddayofyear(temporalRecord.getEndDayOfYear());
    } else {
      occurrenceHdfsRecord.setEnddayofyear(null);
    }

    if (temporalRecord.getEventDate() != null
        && temporalRecord.getEventDate().getGte() != null
        && temporalRecord.getEventDate().getLte() != null) {
      Optional.ofNullable(temporalRecord.getEventDate())
          .ifPresent(
              eventDate -> {
                occurrenceHdfsRecord.setEventdate(eventDate.getInterval());
                occurrenceHdfsRecord.setEventdategte(
                    StringToDateFunctions.getStringToEarliestEpochSeconds(true)
                        .apply(eventDate.getGte()));
                occurrenceHdfsRecord.setEventdatelte(
                    StringToDateFunctions.getStringToLatestEpochSeconds(true)
                        .apply(eventDate.getLte()));
              });
    } else {
      TemporalConverter.from(
              temporalRecord.getYear(), temporalRecord.getMonth(), temporalRecord.getDay())
          .ifPresent(
              eventDate -> {
                occurrenceHdfsRecord.setEventdate(eventDate.toString());
                long instant =
                    ((LocalDate) eventDate)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()
                        .getEpochSecond();
                occurrenceHdfsRecord.setEventdategte(instant);
                occurrenceHdfsRecord.setEventdatelte(instant);
              });
    }

    setCreatedIfGreater(occurrenceHdfsRecord, temporalRecord.getCreated());
    addNonTaxonIssues(temporalRecord.getIssues(), occurrenceHdfsRecord);
  }

  private void mapMultiTaxonRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (multiTaxonRecord == null
        || multiTaxonRecord.getTaxonRecords() == null
        || multiTaxonRecord.getTaxonRecords().isEmpty()) {
      occurrenceHdfsRecord.setClassifications(new HashMap<>());
      return;
    }
    occurrenceHdfsRecord.setChecklistkey(
        multiTaxonRecord.getTaxonRecords().stream()
            .map(TaxonRecord::getDatasetKey)
            .collect(Collectors.toList()));

    occurrenceHdfsRecord.setClassifications(
        multiTaxonRecord.getTaxonRecords().stream()
            .collect(
                Collectors.toMap(
                    TaxonRecord::getDatasetKey,
                    tr ->
                        tr.getClassification().stream()
                            .map(RankedName::getKey)
                            .collect(Collectors.toList()))));

    occurrenceHdfsRecord.setTaxonomicstatuses(
        multiTaxonRecord.getTaxonRecords().stream()
            .collect(
                Collectors.toMap(
                    TaxonRecord::getDatasetKey,
                    tr ->
                        tr.getUsage() != null && tr.getUsage().getStatus() != null
                            ? tr.getUsage().getStatus()
                            : "")));

    occurrenceHdfsRecord.setTaxonomicissue(
        multiTaxonRecord.getTaxonRecords().stream()
            .collect(
                Collectors.toMap(
                    TaxonRecord::getDatasetKey,
                    tr ->
                        tr.getIssues() != null && tr.getIssues().getIssueList() != null
                            ? tr.getIssues().getIssueList()
                            : List.of())));

    occurrenceHdfsRecord.setClassificationdetails(
        multiTaxonRecord.getTaxonRecords().stream()
            .filter(tr -> tr.getDatasetKey() != null)
            .filter(tr -> tr.getUsage() != null)
            .collect(
                Collectors.toMap(
                    TaxonRecord::getDatasetKey, tr -> classificationToMap(extendedRecord, tr))));

    // find the GBIF taxonomy
    Optional<TaxonRecord> gbifRecord =
        multiTaxonRecord.getTaxonRecords().stream()
            .filter(
                tr -> OccurrenceJsonConverter.GBIF_BACKBONE_DATASET_KEY.equals(tr.getDatasetKey()))
            .findFirst();

    gbifRecord.ifPresent(tr -> mapLegacyGbifTaxonRecord(occurrenceHdfsRecord, tr));
  }

  private static Map<String, String> classificationToMap(
      ExtendedRecord verbatim, TaxonRecord taxonRecord) {

    Map<String, String> map = new HashMap<>();

    // Get usage and accepted usage
    RankedNameWithAuthorship usage = taxonRecord.getUsage();
    RankedNameWithAuthorship acceptedUsage = taxonRecord.getAcceptedUsage();

    if (usage != null) {

      // Required taxon keys and names
      map.put(GbifTerm.taxonKey.simpleName().toLowerCase(), usage.getKey());
      map.put(DwcTerm.scientificName.simpleName().toLowerCase(), usage.getName());

      map.put(
          GbifTerm.acceptedTaxonKey.simpleName().toLowerCase(),
          acceptedUsage != null ? acceptedUsage.getKey() : usage.getKey());
      map.put(
          DwcTerm.acceptedNameUsageID.simpleName().toLowerCase(),
          acceptedUsage != null ? acceptedUsage.getKey() : usage.getKey());
      map.put(
          GbifTerm.acceptedScientificName.simpleName().toLowerCase(),
          acceptedUsage != null ? acceptedUsage.getName() : usage.getName());

      // Optional taxonomic fields
      map.put(
          DwcTerm.genericName.simpleName().toLowerCase(),
          usage.getGenericName() != null ? usage.getGenericName() : "");
      map.put(
          DwcTerm.specificEpithet.simpleName().toLowerCase(),
          usage.getSpecificEpithet() != null ? usage.getSpecificEpithet() : "");
      map.put(
          DwcTerm.infraspecificEpithet.simpleName().toLowerCase(),
          usage.getInfraspecificEpithet() != null ? usage.getInfraspecificEpithet() : "");

      map.put(
          DwcTerm.taxonRank.simpleName().toLowerCase(),
          usage.getRank() != null ? usage.getRank() : "");
    }

    extractOptValue(verbatim, DwcTerm.scientificName)
        .ifPresent(s -> map.put(GbifTerm.verbatimScientificName.simpleName().toLowerCase(), s));

    // Classification hierarchy
    taxonRecord
        .getClassification()
        .forEach(
            rankedName -> {
              map.put(rankedName.getRank().toLowerCase(), rankedName.getName());
              map.put(rankedName.getRank().toLowerCase() + "key", rankedName.getKey());
            });

    // Optional IUCN field
    map.put(
        IucnTerm.iucnRedListCategory.simpleName().toLowerCase(),
        taxonRecord.getIucnRedListCategoryCode() != null
            ? taxonRecord.getIucnRedListCategoryCode()
            : "");

    for (String field : REQUIRED_TAXONOMIC_FIELDS) {
      map.putIfAbsent(field, "");
    }

    return map;
  }

  /** Copies the {@link TaxonRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapLegacyGbifTaxonRecord(
      OccurrenceHdfsRecord occurrenceHdfsRecord, TaxonRecord taxonRecord) {
    if (taxonRecord == null) {
      return;
    }
    Optional.ofNullable(taxonRecord.getUsage())
        .ifPresent(x -> occurrenceHdfsRecord.setTaxonkey(x.getKey()));
    if (Objects.nonNull(taxonRecord.getClassification())) {
      taxonRecord
          .getClassification()
          .forEach(
              rankedName -> {
                switch (rankedName.getRank()) {
                  case "KINGDOM":
                    occurrenceHdfsRecord.setKingdom(rankedName.getName());
                    occurrenceHdfsRecord.setKingdomkey(rankedName.getKey());
                    break;
                  case "PHYLUM":
                    occurrenceHdfsRecord.setPhylum(rankedName.getName());
                    occurrenceHdfsRecord.setPhylumkey(rankedName.getKey());
                    break;
                  case "CLASS":
                    occurrenceHdfsRecord.setClass_(rankedName.getName());
                    occurrenceHdfsRecord.setClasskey(rankedName.getKey());
                    break;
                  case "ORDER":
                    occurrenceHdfsRecord.setOrder(rankedName.getName());
                    occurrenceHdfsRecord.setOrderkey(rankedName.getKey());
                    break;
                  case "FAMILY":
                    occurrenceHdfsRecord.setFamily(rankedName.getName());
                    occurrenceHdfsRecord.setFamilykey(rankedName.getKey());
                    break;
                  case "GENUS":
                    occurrenceHdfsRecord.setGenus(rankedName.getName());
                    occurrenceHdfsRecord.setGenuskey(rankedName.getKey());
                    break;
                  case "SUBGENUS":
                    occurrenceHdfsRecord.setSubgenus(rankedName.getName());
                    occurrenceHdfsRecord.setSubgenuskey(rankedName.getKey());
                    break;
                  case "SPECIES":
                    occurrenceHdfsRecord.setSpecies(rankedName.getName());
                    occurrenceHdfsRecord.setSpecieskey(rankedName.getKey());
                    break;
                  default:
                    break;
                }
              });
    }

    if (Objects.nonNull(taxonRecord.getAcceptedUsage())) {
      occurrenceHdfsRecord.setAcceptedscientificname(taxonRecord.getAcceptedUsage().getName());
      occurrenceHdfsRecord.setAcceptednameusageid(taxonRecord.getAcceptedUsage().getKey());
      if (Objects.nonNull(taxonRecord.getAcceptedUsage().getKey())) {
        occurrenceHdfsRecord.setAcceptedtaxonkey(taxonRecord.getAcceptedUsage().getKey());
      }
      Optional.ofNullable(taxonRecord.getAcceptedUsage().getRank())
          .ifPresent(occurrenceHdfsRecord::setTaxonrank);
    } else if (Objects.nonNull(taxonRecord.getUsage())
        && !taxonRecord.getUsage().getKey().equals("0")) {
      // if the acceptedUsage is null we use the usage as the accepted as longs as it's not
      // incertae sedis
      occurrenceHdfsRecord.setAcceptedtaxonkey(taxonRecord.getUsage().getKey());
      occurrenceHdfsRecord.setAcceptedscientificname(taxonRecord.getUsage().getName());
      occurrenceHdfsRecord.setAcceptednameusageid(taxonRecord.getUsage().getKey());
    }

    if (Objects.nonNull(taxonRecord.getUsage())) {
      occurrenceHdfsRecord.setTaxonkey(taxonRecord.getUsage().getKey());
      occurrenceHdfsRecord.setScientificname(taxonRecord.getUsage().getName());
      Optional.ofNullable(taxonRecord.getUsage().getRank())
          .ifPresent(occurrenceHdfsRecord::setTaxonrank);
      occurrenceHdfsRecord.setTaxonomicstatus(
          taxonRecord.getUsage().getStatus() != null ? taxonRecord.getUsage().getStatus() : "");
    }

    if (Objects.nonNull(taxonRecord.getUsageParsedName())
        && Objects.nonNull(taxonRecord.getUsage())) {
      String rank = taxonRecord.getUsage().getRank();
      if (Rank.GENUS.compareTo(Rank.valueOf(rank)) <= 0) {
        occurrenceHdfsRecord.setGenericname(
            Objects.nonNull(taxonRecord.getUsageParsedName().getGenus())
                ? taxonRecord.getUsageParsedName().getGenus()
                : taxonRecord.getUsageParsedName().getUninomial());
      }

      if (Rank.SPECIES.compareTo(Rank.valueOf(rank)) <= 0) {
        occurrenceHdfsRecord.setSpecificepithet(
            taxonRecord.getUsageParsedName().getSpecificEpithet());
      }

      if (Rank.INFRASPECIFIC_NAME.compareTo(Rank.valueOf(rank)) <= 0) {
        occurrenceHdfsRecord.setInfraspecificepithet(
            taxonRecord.getUsageParsedName().getInfraspecificEpithet());
      }
    }

    setCreatedIfGreater(occurrenceHdfsRecord, taxonRecord.getCreated());

    occurrenceHdfsRecord.setIucnredlistcategory(taxonRecord.getIucnRedListCategoryCode());

    addTaxonIssues(taxonRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link GrscicollRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapGrscicollRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (grscicollRecord == null) {
      return;
    }

    if (grscicollRecord.getInstitutionMatch() != null) {
      String institutionKey = grscicollRecord.getInstitutionMatch().getKey();
      if (institutionKey != null) {
        occurrenceHdfsRecord.setInstitutionkey(institutionKey);
      }
    }

    if (grscicollRecord.getCollectionMatch() != null) {
      String collectionKey = grscicollRecord.getCollectionMatch().getKey();
      if (collectionKey != null) {
        occurrenceHdfsRecord.setCollectionkey(collectionKey);
      }
    }

    addNonTaxonIssues(grscicollRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link IdentifierRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapIdentifierRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (identifierRecord == null) {
      return;
    }
    if (Objects.nonNull(identifierRecord.getInternalId())) {
      occurrenceHdfsRecord.setGbifid(identifierRecord.getInternalId());
    }

    setCreatedIfGreater(occurrenceHdfsRecord, identifierRecord.getFirstLoaded());
    addNonTaxonIssues(identifierRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link ClusteringRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapClusteringRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (clusteringRecord == null) {
      return;
    }
    occurrenceHdfsRecord.setIsincluster(clusteringRecord.getIsClustered());

    setCreatedIfGreater(occurrenceHdfsRecord, clusteringRecord.getCreated());
    addNonTaxonIssues(clusteringRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link BasicRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapBasicRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (basicRecord == null) {
      return;
    }

    occurrenceHdfsRecord.setBasisofrecord(basicRecord.getBasisOfRecord());
    occurrenceHdfsRecord.setIndividualcount(basicRecord.getIndividualCount());
    occurrenceHdfsRecord.setReferences(basicRecord.getReferences());
    occurrenceHdfsRecord.setTypifiedname(basicRecord.getTypifiedName());
    occurrenceHdfsRecord.setOrganismquantity(basicRecord.getOrganismQuantity());
    occurrenceHdfsRecord.setOrganismquantitytype(basicRecord.getOrganismQuantityType());
    occurrenceHdfsRecord.setSamplesizeunit(basicRecord.getSampleSizeUnit());
    occurrenceHdfsRecord.setSamplesizevalue(basicRecord.getSampleSizeValue());
    occurrenceHdfsRecord.setRelativeorganismquantity(basicRecord.getRelativeOrganismQuantity());
    occurrenceHdfsRecord.setOccurrencestatus(basicRecord.getOccurrenceStatus());
    occurrenceHdfsRecord.setDatasetid(basicRecord.getDatasetID());
    occurrenceHdfsRecord.setDatasetname(basicRecord.getDatasetName());
    occurrenceHdfsRecord.setOthercatalognumbers(basicRecord.getOtherCatalogNumbers());
    occurrenceHdfsRecord.setRecordedby(basicRecord.getRecordedBy());
    occurrenceHdfsRecord.setIdentifiedby(basicRecord.getIdentifiedBy());
    occurrenceHdfsRecord.setPreparations(basicRecord.getPreparations());
    occurrenceHdfsRecord.setSamplingprotocol(basicRecord.getSamplingProtocol());
    occurrenceHdfsRecord.setIssequenced(basicRecord.getIsSequenced());
    occurrenceHdfsRecord.setAssociatedsequences(basicRecord.getAssociatedSequences());

    // Vocabulary controlled
    Optional.ofNullable(basicRecord.getSex())
        .ifPresent(
            c ->
                occurrenceHdfsRecord.setSex(
                    Sex.newBuilder()
                        .setConcept(c.getConcept())
                        .setLineage(c.getLineage())
                        .build()));

    Optional.ofNullable(basicRecord.getEstablishmentMeans())
        .ifPresent(
            c ->
                occurrenceHdfsRecord.setEstablishmentmeans(
                    EstablishmentMeans.newBuilder()
                        .setConcept(c.getConcept())
                        .setLineage(c.getLineage())
                        .build()));

    Optional.ofNullable(basicRecord.getLifeStage())
        .ifPresent(
            c ->
                occurrenceHdfsRecord.setLifestage(
                    LifeStage.newBuilder()
                        .setConcept(c.getConcept())
                        .setLineage(c.getLineage())
                        .build()));

    Optional.ofNullable(basicRecord.getPathway())
        .ifPresent(
            c ->
                occurrenceHdfsRecord.setPathway(
                    Pathway.newBuilder()
                        .setConcept(c.getConcept())
                        .setLineage(c.getLineage())
                        .build()));

    Optional.ofNullable(basicRecord.getDegreeOfEstablishment())
        .ifPresent(
            c ->
                occurrenceHdfsRecord.setDegreeofestablishment(
                    DegreeOfEstablishment.newBuilder()
                        .setConcept(c.getConcept())
                        .setLineage(c.getLineage())
                        .build()));

    Optional.ofNullable(basicRecord.getTypeStatus())
        .filter(ts -> !ts.isEmpty())
        .ifPresent(
            c -> {
              List<String> allConcepts =
                  c.stream()
                      .map(org.gbif.pipelines.io.avro.VocabularyConcept::getConcept)
                      .collect(Collectors.toList());

              List<String> allParents =
                  c.stream().flatMap(c2 -> c2.getLineage().stream()).collect(Collectors.toList());

              occurrenceHdfsRecord.setTypestatus(
                  TypeStatus.newBuilder().setConcepts(allConcepts).setLineage(allParents).build());
            });

    // Others
    Optional.ofNullable(basicRecord.getRecordedByIds())
        .ifPresent(
            uis ->
                occurrenceHdfsRecord.setRecordedbyid(
                    uis.stream().map(AgentIdentifier::getValue).collect(Collectors.toList())));

    Optional.ofNullable(basicRecord.getIdentifiedByIds())
        .ifPresent(
            uis ->
                occurrenceHdfsRecord.setIdentifiedbyid(
                    uis.stream().map(AgentIdentifier::getValue).collect(Collectors.toList())));

    if (basicRecord.getLicense() != null
        && !License.UNSUPPORTED.name().equals(basicRecord.getLicense())
        && !License.UNSPECIFIED.name().equals(basicRecord.getLicense())) {
      occurrenceHdfsRecord.setLicense(basicRecord.getLicense());
    }

    mapGeologicalContext(occurrenceHdfsRecord);

    setCreatedIfGreater(occurrenceHdfsRecord, basicRecord.getCreated());
    addNonTaxonIssues(basicRecord.getIssues(), occurrenceHdfsRecord);
  }

  private void mapGeologicalContext(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    GeologicalContext gc = basicRecord.getGeologicalContext();
    if (gc != null) {

      Optional.ofNullable(gc.getEarliestEonOrLowestEonothem())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setEarliesteonorlowesteonothem(
                      EarliestEonOrLowestEonothem.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getLatestEonOrHighestEonothem())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setLatesteonorhighesteonothem(
                      LatestEonOrHighestEonothem.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getEarliestEraOrLowestErathem())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setEarliesteraorlowesterathem(
                      EarliestEraOrLowestErathem.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getLatestEraOrHighestErathem())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setLatesteraorhighesterathem(
                      LatestEraOrHighestErathem.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getEarliestPeriodOrLowestSystem())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setEarliestperiodorlowestsystem(
                      EarliestPeriodOrLowestSystem.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getLatestPeriodOrHighestSystem())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setLatestperiodorhighestsystem(
                      LatestPeriodOrHighestSystem.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getEarliestEpochOrLowestSeries())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setEarliestepochorlowestseries(
                      EarliestEpochOrLowestSeries.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getLatestEpochOrHighestSeries())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setLatestepochorhighestseries(
                      LatestEpochOrHighestSeries.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getEarliestAgeOrLowestStage())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setEarliestageorloweststage(
                      EarliestAgeOrLowestStage.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      Optional.ofNullable(gc.getLatestAgeOrHighestStage())
          .ifPresent(
              c ->
                  occurrenceHdfsRecord.setLatestageorhigheststage(
                      LatestAgeOrHighestStage.newBuilder()
                          .setConcept(c.getConcept())
                          .setLineage(c.getLineage())
                          .build()));

      occurrenceHdfsRecord.setLowestbiostratigraphiczone(gc.getLowestBiostratigraphicZone());
      occurrenceHdfsRecord.setHighestbiostratigraphiczone(gc.getHighestBiostratigraphicZone());
      occurrenceHdfsRecord.setGroup(gc.getGroup());
      occurrenceHdfsRecord.setFormation(gc.getFormation());
      occurrenceHdfsRecord.setMember(gc.getMember());
      occurrenceHdfsRecord.setBed(gc.getBed());

      occurrenceHdfsRecord.setLithostratigraphy(
          Stream.of(gc.getBed(), gc.getFormation(), gc.getGroup(), gc.getMember())
              .filter(Objects::nonNull)
              .collect(Collectors.toList()));

      occurrenceHdfsRecord.setBiostratigraphy(
          Stream.of(gc.getLowestBiostratigraphicZone(), gc.getHighestBiostratigraphicZone())
              .filter(Objects::nonNull)
              .collect(Collectors.toList()));

      if (gc.getStartAge() != null && gc.getEndAge() != null) {
        Optional.ofNullable(gc.getStartAge())
            .ifPresent(
                s ->
                    occurrenceHdfsRecord.setGeologicaltime(
                        GeologicalTime.newBuilder()
                            .setLte(gc.getStartAge())
                            .setGt(gc.getEndAge())
                            .build()));
      }
    }
  }

  /**
   * From a {@link Schema.Field} copies it value into a the {@link OccurrenceHdfsRecord} field using
   * the recognized data type.
   *
   * @param occurrenceHdfsRecord target record
   * @param fieldName {@link OccurrenceHdfsRecord} field/property name
   * @param value field data/value
   */
  private static void setHdfsRecordField(
      OccurrenceHdfsRecord occurrenceHdfsRecord, Field field, String fieldName, String value) {
    try {
      Type fieldType = field.getGenericType();
      if (fieldType.equals(Integer.class)) {
        PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Integer.valueOf(value));
      } else if (fieldType.equals(Long.class)) {
        PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Long.valueOf(value));
      } else if (fieldType.equals(Boolean.class)) {
        PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Boolean.valueOf(value));
      } else if (fieldType.equals(Double.class)) {
        PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Double.valueOf(value));
      } else if (fieldType.equals(Float.class)) {
        PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Float.valueOf(value));
      } else {
        PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, value);
      }
    } catch (Exception ex) {
      log.debug(
          "Ignoring error setting field {}, field name {}, value. Exception: {}",
          field,
          fieldName,
          value);
    }
  }

  /** Copies the {@link ExtendedRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapExtendedRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (extendedRecord == null) {
      return;
    }

    extendedRecord.getCoreTerms().forEach((k, v) -> mapTerm(k, v, occurrenceHdfsRecord));

    List<String> extensions =
        extendedRecord.getExtensions().entrySet().stream()
            .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
            .map(Entry::getKey)
            .collect(Collectors.toList());
    occurrenceHdfsRecord.setDwcaextension(extensions);
  }

  /** Copies the {@link EventCoreRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapEventCoreRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (eventCoreRecord != null && eventCoreRecord.getCreated() != null) {
      if (eventCoreRecord.getParentsLineage() != null) {
        occurrenceHdfsRecord.setParenteventgbifid(
            eventCoreRecord.getParentsLineage().stream()
                .map(
                    pl ->
                        ParentEventGbifId.newBuilder()
                            .setId(pl.getId())
                            .setEventtype(pl.getEventType())
                            .build())
                .collect(Collectors.toList()));
      }
      if (eventCoreRecord.getEventType() != null) {
        occurrenceHdfsRecord.setEventtype(
            EventType.newBuilder()
                .setConcept(eventCoreRecord.getEventType().getConcept())
                .setLineage(eventCoreRecord.getEventType().getLineage())
                .build());
      }
    }
  }

  private void mapTerm(String k, String v, OccurrenceHdfsRecord occurrenceHdfsRecord) {
    Term term = TERM_FACTORY.findTerm(k);

    if (term == null) {
      return;
    }

    if (TermUtils.verbatimTerms().contains(term)) {
      Optional.ofNullable(verbatimSchemaField(term))
          .ifPresent(
              field -> {
                try {
                  String verbatimField =
                      "V" + StringUtils.capitalize(term.simpleName().toLowerCase());

                  // special case for class, as always
                  if (DwcTerm.class_ == term) {
                    verbatimField = "VClass_";
                  }

                  PropertyUtils.setProperty(occurrenceHdfsRecord, verbatimField, v);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }

    if (!TermUtils.isInterpretedSourceTerm(term)) {
      Optional.ofNullable(interpretedSchemaField(term))
          .ifPresent(
              field -> {
                // Fields that were set by other mappers are ignored

                // Use reflection to get the value (previously used names from Avro schema)
                String methodName =
                    "get"
                        + Character.toUpperCase(field.getName().charAt(0))
                        + field.getName().substring(1);
                try {
                  Method method = occurrenceHdfsRecord.getClass().getMethod(methodName);
                  if (Objects.isNull(method.invoke(occurrenceHdfsRecord))) {
                    String interpretedFieldname = field.getName();
                    if (DcTerm.abstract_ == term) {
                      interpretedFieldname = "abstract$";
                    } else if (DwcTerm.class_ == term) {
                      interpretedFieldname = "class$";
                    } else if (DwcTerm.group == term) {
                      interpretedFieldname = "group";
                    } else if (DwcTerm.order == term) {
                      interpretedFieldname = "order";
                    } else if (DcTerm.date == term) {
                      interpretedFieldname = "date";
                    } else if (DcTerm.format == term) {
                      interpretedFieldname = "format";
                    }
                    setHdfsRecordField(occurrenceHdfsRecord, field, interpretedFieldname, v);
                  }
                } catch (IllegalAccessException
                    | InvocationTargetException
                    | NoSuchMethodException ex) {
                  throw new RuntimeException(ex);
                }
              });
    }
  }

  /**
   * Collects the {@link MultimediaRecord} mediaTypes data into the {@link
   * OccurrenceHdfsRecord#setMediatype(List)}.
   */
  private void mapMultimediaRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (multimediaRecord == null) {
      return;
    }
    // media types
    List<String> mediaTypes =
        multimediaRecord.getMultimediaItems().stream()
            .map(Multimedia::getType)
            .filter(type -> !Strings.isNullOrEmpty(type))
            .map(TextNode::valueOf)
            .map(TextNode::asText)
            .collect(Collectors.toList());

    occurrenceHdfsRecord.setExtMultimedia(
        base64Encode(MediaSerDeser.multimediaToJson(multimediaRecord.getMultimediaItems())));

    setCreatedIfGreater(occurrenceHdfsRecord, multimediaRecord.getCreated());
    occurrenceHdfsRecord.setMediatype(mediaTypes);

    addNonTaxonIssues(multimediaRecord.getIssues(), occurrenceHdfsRecord);
  }

  public static String base64Encode(String original) {
    if (original == null) {
      return null;
    }
    return Base64.getEncoder().encodeToString(original.getBytes(StandardCharsets.UTF_8));
  }

  private void mapDnaDerivedDataRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (dnaDerivedDataRecord == null) {
      return;
    }

    if (dnaDerivedDataRecord.getDnaDerivedDataItems() != null
        && !dnaDerivedDataRecord.getDnaDerivedDataItems().isEmpty()) {
      occurrenceHdfsRecord.setDnasequenceid(
          new ArrayList<>(
              dnaDerivedDataRecord.getDnaDerivedDataItems().stream()
                  .map(DnaDerivedData::getDnaSequenceID)
                  .collect(Collectors.toSet())));
    }
  }

  private void mapHumboldtRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (humboldtRecord == null || humboldtRecord.getCreated() == null) {
      return;
    }

    if (humboldtRecord.getHumboldtItems() != null) {

      Function<List<VocabularyConcept>, HumboldtJsonView.VocabularyList> convertVocabList =
          list -> {
            List<String> allConcepts =
                list.stream()
                    .map(org.gbif.pipelines.io.avro.VocabularyConcept::getConcept)
                    .collect(Collectors.toList());

            List<String> allParents =
                list.stream().flatMap(c -> c.getLineage().stream()).collect(Collectors.toList());

            HumboldtJsonView.VocabularyList vl = new HumboldtJsonView.VocabularyList();
            vl.setConcepts(allConcepts);
            vl.setLineage(allParents);
            return vl;
          };

      Function<List<TaxonHumboldtRecord>, Map<String, Map<String, List<String>>>>
          convertToTaxonMap =
              r -> {
                Map<String, Map<String, List<String>>> valuesAsList = new HashMap<>();

                r.stream()
                    .filter(v -> v.getChecklistKey() != null)
                    .forEach(
                        t -> {
                          Map<String, List<String>> values =
                              valuesAsList.computeIfAbsent(
                                  t.getChecklistKey(), k -> new HashMap<>());

                          if (t.getUsage() != null) {
                            values
                                .computeIfAbsent("usagekey", k -> new ArrayList<>())
                                .add(t.getUsage().getKey());
                            values
                                .computeIfAbsent("usagename", k -> new ArrayList<>())
                                .add(t.getUsage().getName());
                            values
                                .computeIfAbsent("usagerank", k -> new ArrayList<>())
                                .add(t.getUsage().getRank());
                          }
                          if (t.getAcceptedUsage() != null) {
                            values
                                .computeIfAbsent("acceptedusagekey", k -> new ArrayList<>())
                                .add(t.getAcceptedUsage().getKey());
                            values
                                .computeIfAbsent("acceptedusagename", k -> new ArrayList<>())
                                .add(t.getAcceptedUsage().getName());
                            values
                                .computeIfAbsent("acceptedusagerank", k -> new ArrayList<>())
                                .add(t.getAcceptedUsage().getRank());
                          }

                          values
                              .computeIfAbsent("iucnRedListCategoryCode", k -> new ArrayList<>())
                              .add(t.getIucnRedListCategoryCode());

                          values
                              .computeIfAbsent("taxonkeys", k -> new ArrayList<>())
                              .addAll(
                                  t.getClassification().stream()
                                      .map(RankedName::getKey)
                                      .collect(Collectors.toSet()));
                          values
                              .computeIfAbsent("taxonomicissue", k -> new ArrayList<>())
                              .addAll(t.getIssues().getIssueList());

                          t.getClassification()
                              .forEach(
                                  rn -> {
                                    values
                                        .computeIfAbsent(
                                            rn.getRank().toLowerCase(), k -> new ArrayList<>())
                                        .add(rn.getName());
                                    values
                                        .computeIfAbsent(
                                            rn.getRank().toLowerCase() + "key",
                                            k -> new ArrayList<>())
                                        .add(rn.getKey());
                                  });
                        });
                return valuesAsList;
              };

      List<HumboldtJsonView> jsonViews =
          humboldtRecord.getHumboldtItems().stream()
              .map(
                  h -> {
                    HumboldtJsonView jsonView = new HumboldtJsonView();
                    jsonView.setHumboldt(h);

                    if (h.getTargetLifeStageScope() != null) {
                      jsonView.setTargetLifeStageScope(
                          convertVocabList.apply(h.getTargetLifeStageScope()));
                    }
                    if (h.getExcludedLifeStageScope() != null) {
                      jsonView.setExcludedLifeStageScope(
                          convertVocabList.apply(h.getExcludedLifeStageScope()));
                    }
                    if (h.getTargetDegreeOfEstablishmentScope() != null) {
                      jsonView.setTargetDegreeOfEstablishmentScope(
                          convertVocabList.apply(h.getTargetDegreeOfEstablishmentScope()));
                    }
                    if (h.getExcludedDegreeOfEstablishmentScope() != null) {
                      jsonView.setExcludedDegreeOfEstablishmentScope(
                          convertVocabList.apply(h.getExcludedDegreeOfEstablishmentScope()));
                    }

                    if (h.getTargetTaxonomicScope() != null) {
                      jsonView.setTargetTaxonomicScope(
                          convertToTaxonMap.apply(h.getTargetTaxonomicScope()));
                    }
                    if (h.getExcludedTaxonomicScope() != null) {
                      jsonView.setExcludedTaxonomicScope(
                          convertToTaxonMap.apply(h.getExcludedTaxonomicScope()));
                    }
                    if (h.getAbsentTaxa() != null) {
                      jsonView.setAbsentTaxa(convertToTaxonMap.apply(h.getAbsentTaxa()));
                    }
                    if (h.getNonTargetTaxa() != null) {
                      jsonView.setNonTargetTaxa(convertToTaxonMap.apply(h.getNonTargetTaxa()));
                    }

                    return jsonView;
                  })
              .collect(Collectors.toList());

      occurrenceHdfsRecord.setExtHumboldt(base64Encode(MediaSerDeser.humboldtToJson(jsonViews)));
    }

    addNonTaxonIssues(humboldtRecord.getIssues(), occurrenceHdfsRecord);

    // Add taxonomic issues from humboldtRecord
    humboldtRecord
        .getHumboldtItems()
        .forEach(
            h ->
                h.getTargetTaxonomicScope()
                    .forEach(ir -> addTaxonIssues(ir.getIssues(), occurrenceHdfsRecord)));
  }

  /** Gets the {@link Schema.Field} associated to a verbatim term. */
  private static Field verbatimSchemaField(Term term) {
    try {

      if (term == DwcTerm.class_) {
        return OccurrenceHdfsRecord.class.getDeclaredField("vClass_");
      }

      return OccurrenceHdfsRecord.class.getDeclaredField(
          "v" + StringUtils.capitalize(term.simpleName().toLowerCase()));
    } catch (NoSuchFieldException e) {
      return null; // Field not found
    }
  }

  /** Gets the {@link Schema.Field} associated to a interpreted term. */
  private static Field interpretedSchemaField(Term term) {
    try {
      return OccurrenceHdfsRecord.class.getDeclaredField(HiveColumns.columnFor(term));
    } catch (NoSuchFieldException e) {
      return null; // Field not found
    }
  }
}
