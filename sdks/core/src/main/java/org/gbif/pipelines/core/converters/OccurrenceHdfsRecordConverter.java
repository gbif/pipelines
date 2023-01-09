package org.gbif.pipelines.core.converters;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.beanutils.PropertyUtils;
import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.utils.MediaSerDeser;
import org.gbif.pipelines.core.utils.TemporalConverter;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.DegreeOfEstablishment;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.EstablishmentMeans;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.EventType;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.LifeStage;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.ParentEventGbifId;
import org.gbif.pipelines.io.avro.Pathway;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;

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
  private final TaxonRecord taxonRecord;
  private final GrscicollRecord grscicollRecord;
  private final TemporalRecord temporalRecord;
  private final MetadataRecord metadataRecord;
  private final MultimediaRecord multimediaRecord;
  private final EventCoreRecord eventCoreRecord;

  /**
   * Collects data from {@link SpecificRecordBase} instances into a {@link OccurrenceHdfsRecord}.
   *
   * @return a {@link OccurrenceHdfsRecord} instance based on the input records
   */
  public OccurrenceHdfsRecord convert() {
    OccurrenceHdfsRecord occurrenceHdfsRecord = new OccurrenceHdfsRecord();
    occurrenceHdfsRecord.setIssue(new ArrayList<>());

    // Order is important
    mapIdentifierRecord(occurrenceHdfsRecord);
    mapClusteringRecord(occurrenceHdfsRecord);
    mapBasicRecord(occurrenceHdfsRecord);
    mapMetadataRecord(occurrenceHdfsRecord);
    mapTemporalRecord(occurrenceHdfsRecord);
    mapLocationRecord(occurrenceHdfsRecord);
    mapTaxonRecord(occurrenceHdfsRecord);
    mapGrscicollRecord(occurrenceHdfsRecord);
    mapMultimediaRecord(occurrenceHdfsRecord);
    mapExtendedRecord(occurrenceHdfsRecord);
    mapEventCoreRecord(occurrenceHdfsRecord);
    mapProjectIds(occurrenceHdfsRecord);

    // The id (the <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and
    // could only have been
    // used for "un-starring" a DWCA star record. However, we've exposed it as DcTerm.identifier for
    // a long time in
    // our public API v1, so we continue to do this.
    if (extendedRecord != null && identifierRecord != null) {
      setIdentifier(identifierRecord, extendedRecord, occurrenceHdfsRecord);
    }

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
  private static void addIssues(
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

    setCreatedIfGreater(occurrenceHdfsRecord, locationRecord.getCreated());
    addIssues(locationRecord.getIssues(), occurrenceHdfsRecord);
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
    addIssues(metadataRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link TemporalRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapTemporalRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (temporalRecord == null) {
      return;
    }
    Optional.ofNullable(temporalRecord.getDateIdentified())
        .map(StringToDateFunctions.getStringToDateFn())
        .ifPresent(date -> occurrenceHdfsRecord.setDateidentified(date.getTime()));
    Optional.ofNullable(temporalRecord.getModified())
        .map(StringToDateFunctions.getStringToDateFn())
        .ifPresent(date -> occurrenceHdfsRecord.setModified(date.getTime()));
    occurrenceHdfsRecord.setDay(temporalRecord.getDay());
    occurrenceHdfsRecord.setMonth(temporalRecord.getMonth());
    occurrenceHdfsRecord.setYear(temporalRecord.getYear());

    if (Objects.nonNull(temporalRecord.getStartDayOfYear())) {
      occurrenceHdfsRecord.setStartdayofyear(temporalRecord.getStartDayOfYear().toString());
    } else {
      occurrenceHdfsRecord.setStartdayofyear(null);
    }

    if (Objects.nonNull(temporalRecord.getEndDayOfYear())) {
      occurrenceHdfsRecord.setEnddayofyear(temporalRecord.getEndDayOfYear().toString());
    } else {
      occurrenceHdfsRecord.setEnddayofyear(null);
    }

    if (temporalRecord.getEventDate() != null && temporalRecord.getEventDate().getGte() != null) {
      Optional.ofNullable(temporalRecord.getEventDate().getGte())
          .map(StringToDateFunctions.getStringToDateFn(true))
          .ifPresent(eventDate -> occurrenceHdfsRecord.setEventdate(eventDate.getTime()));
    } else {
      TemporalConverter.from(
              temporalRecord.getYear(), temporalRecord.getMonth(), temporalRecord.getDay())
          .map(StringToDateFunctions.getTemporalToDateFn())
          .ifPresent(eventDate -> occurrenceHdfsRecord.setEventdate(eventDate.getTime()));
    }

    setCreatedIfGreater(occurrenceHdfsRecord, temporalRecord.getCreated());
    addIssues(temporalRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link TaxonRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapTaxonRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
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
                  case KINGDOM:
                    occurrenceHdfsRecord.setKingdom(rankedName.getName());
                    occurrenceHdfsRecord.setKingdomkey(rankedName.getKey());
                    break;
                  case PHYLUM:
                    occurrenceHdfsRecord.setPhylum(rankedName.getName());
                    occurrenceHdfsRecord.setPhylumkey(rankedName.getKey());
                    break;
                  case CLASS:
                    occurrenceHdfsRecord.setClass$(rankedName.getName());
                    occurrenceHdfsRecord.setClasskey(rankedName.getKey());
                    break;
                  case ORDER:
                    occurrenceHdfsRecord.setOrder(rankedName.getName());
                    occurrenceHdfsRecord.setOrderkey(rankedName.getKey());
                    break;
                  case FAMILY:
                    occurrenceHdfsRecord.setFamily(rankedName.getName());
                    occurrenceHdfsRecord.setFamilykey(rankedName.getKey());
                    break;
                  case GENUS:
                    occurrenceHdfsRecord.setGenus(rankedName.getName());
                    occurrenceHdfsRecord.setGenuskey(rankedName.getKey());
                    break;
                  case SUBGENUS:
                    occurrenceHdfsRecord.setSubgenus(rankedName.getName());
                    occurrenceHdfsRecord.setSubgenuskey(rankedName.getKey());
                    break;
                  case SPECIES:
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
      occurrenceHdfsRecord.setAcceptednameusageid(
          taxonRecord.getAcceptedUsage().getKey().toString());
      if (Objects.nonNull(taxonRecord.getAcceptedUsage().getKey())) {
        occurrenceHdfsRecord.setAcceptedtaxonkey(taxonRecord.getAcceptedUsage().getKey());
      }
      Optional.ofNullable(taxonRecord.getAcceptedUsage().getRank())
          .ifPresent(r -> occurrenceHdfsRecord.setTaxonrank(r.name()));
    } else if (Objects.nonNull(taxonRecord.getUsage()) && taxonRecord.getUsage().getKey() != 0) {
      // if the acceptedUsage is null we use the usage as the accepted as longs as it's not
      // incertidae sedis
      occurrenceHdfsRecord.setAcceptedtaxonkey(taxonRecord.getUsage().getKey());
      occurrenceHdfsRecord.setAcceptedscientificname(taxonRecord.getUsage().getName());
      occurrenceHdfsRecord.setAcceptednameusageid(taxonRecord.getUsage().getKey().toString());
    }

    if (Objects.nonNull(taxonRecord.getUsage())) {
      occurrenceHdfsRecord.setTaxonkey(taxonRecord.getUsage().getKey());
      occurrenceHdfsRecord.setScientificname(taxonRecord.getUsage().getName());
      Optional.ofNullable(taxonRecord.getUsage().getRank())
          .ifPresent(r -> occurrenceHdfsRecord.setTaxonrank(r.name()));
    }

    if (Objects.nonNull(taxonRecord.getUsageParsedName())) {
      occurrenceHdfsRecord.setGenericname(
          Objects.nonNull(taxonRecord.getUsageParsedName().getGenus())
              ? taxonRecord.getUsageParsedName().getGenus()
              : taxonRecord.getUsageParsedName().getUninomial());
      occurrenceHdfsRecord.setSpecificepithet(
          taxonRecord.getUsageParsedName().getSpecificEpithet());
      occurrenceHdfsRecord.setInfraspecificepithet(
          taxonRecord.getUsageParsedName().getInfraspecificEpithet());
    }

    Optional.ofNullable(taxonRecord.getDiagnostics())
        .map(Diagnostic::getStatus)
        .ifPresent(d -> occurrenceHdfsRecord.setTaxonomicstatus(d.name()));

    setCreatedIfGreater(occurrenceHdfsRecord, taxonRecord.getCreated());

    occurrenceHdfsRecord.setIucnredlistcategory(taxonRecord.getIucnRedListCategoryCode());

    addIssues(taxonRecord.getIssues(), occurrenceHdfsRecord);
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

    addIssues(grscicollRecord.getIssues(), occurrenceHdfsRecord);
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
    addIssues(identifierRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link ClusteringRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapClusteringRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (clusteringRecord == null) {
      return;
    }
    occurrenceHdfsRecord.setIsincluster(clusteringRecord.getIsClustered());

    setCreatedIfGreater(occurrenceHdfsRecord, clusteringRecord.getCreated());
    addIssues(clusteringRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Copies the {@link BasicRecord} data into the {@link OccurrenceHdfsRecord}. */
  private void mapBasicRecord(OccurrenceHdfsRecord occurrenceHdfsRecord) {
    if (basicRecord == null) {
      return;
    }

    occurrenceHdfsRecord.setBasisofrecord(basicRecord.getBasisOfRecord());
    occurrenceHdfsRecord.setIndividualcount(basicRecord.getIndividualCount());
    occurrenceHdfsRecord.setReferences(basicRecord.getReferences());
    occurrenceHdfsRecord.setSex(basicRecord.getSex());
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
    occurrenceHdfsRecord.setTypestatus(basicRecord.getTypeStatus());

    // Vocabulary controlled
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

    setCreatedIfGreater(occurrenceHdfsRecord, basicRecord.getCreated());
    addIssues(basicRecord.getIssues(), occurrenceHdfsRecord);
  }

  /**
   * The id (the <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and could
   * only have been used for "un-starring" a DWCA star record. However, we've exposed it as
   * DcTerm.identifier for a long time in our public API v1, so we continue to do this.the id (the
   * <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and could only have
   * been used for "un-starring" a DWCA star record. However, we've exposed it as DcTerm.identifier
   * for a long time in our public API v1, so we continue to do this.
   */
  private static void setIdentifier(
      IdentifierRecord ir, ExtendedRecord er, OccurrenceHdfsRecord occurrenceHdfsRecord) {

    String institutionCode = er.getCoreTerms().get(DwcTerm.institutionCode.qualifiedName());
    String collectionCode = er.getCoreTerms().get(DwcTerm.collectionCode.qualifiedName());
    String catalogNumber = er.getCoreTerms().get(DwcTerm.catalogNumber.qualifiedName());

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    String triplet =
        String.join(":", "urn:catalog", institutionCode, collectionCode, catalogNumber);
    String gbifId = Optional.ofNullable(ir.getInternalId()).map(Object::toString).orElse("");

    String occId = er.getCoreTerms().get(DwcTerm.occurrenceID.qualifiedName());

    if (!ir.getId().equals(gbifId)
        && (!Strings.isNullOrEmpty(occId) || !ir.getId().equals(triplet))) {
      occurrenceHdfsRecord.setIdentifier(ir.getId());
      occurrenceHdfsRecord.setVIdentifier(ir.getId());
    }
  }

  /**
   * From a {@link Schema.Field} copies it value into a the {@link OccurrenceHdfsRecord} field using
   * the recognized data type.
   *
   * @param occurrenceHdfsRecord target record
   * @param avroField field to be copied
   * @param fieldName {@link OccurrenceHdfsRecord} field/property name
   * @param value field data/value
   */
  private static void setHdfsRecordField(
      OccurrenceHdfsRecord occurrenceHdfsRecord,
      Schema.Field avroField,
      String fieldName,
      String value) {
    try {
      Schema.Type fieldType = avroField.schema().getType();
      if (Schema.Type.UNION == avroField.schema().getType()) {
        fieldType = avroField.schema().getTypes().get(0).getType();
      }
      switch (fieldType) {
        case INT:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Integer.valueOf(value));
          break;
        case LONG:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Long.valueOf(value));
          break;
        case BOOLEAN:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Boolean.valueOf(value));
          break;
        case DOUBLE:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Double.valueOf(value));
          break;
        case FLOAT:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, Float.valueOf(value));
          break;
        default:
          PropertyUtils.setProperty(occurrenceHdfsRecord, fieldName, value);
          break;
      }
    } catch (Exception ex) {
      log.error("Ignoring error setting field {}", avroField, ex);
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
                String verbatimField =
                    "V" + field.name().substring(2, 3).toUpperCase() + field.name().substring(3);
                setHdfsRecordField(occurrenceHdfsRecord, field, verbatimField, v);
              });
    }

    if (!TermUtils.isInterpretedSourceTerm(term)) {
      Optional.ofNullable(interpretedSchemaField(term))
          .ifPresent(
              field -> {
                // Fields that were set by other mappers are ignored
                if (Objects.isNull(occurrenceHdfsRecord.get(field.name()))) {
                  String interpretedFieldname = field.name();
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
        MediaSerDeser.toJson(multimediaRecord.getMultimediaItems()));

    setCreatedIfGreater(occurrenceHdfsRecord, multimediaRecord.getCreated());
    occurrenceHdfsRecord.setMediatype(mediaTypes);

    addIssues(multimediaRecord.getIssues(), occurrenceHdfsRecord);
  }

  /** Gets the {@link Schema.Field} associated to a verbatim term. */
  private static Schema.Field verbatimSchemaField(Term term) {
    return OccurrenceHdfsRecord.SCHEMA$.getField("v_" + term.simpleName().toLowerCase());
  }

  /** Gets the {@link Schema.Field} associated to a interpreted term. */
  private static Schema.Field interpretedSchemaField(Term term) {
    return OccurrenceHdfsRecord.SCHEMA$.getField(HiveColumns.columnFor(term));
  }
}
