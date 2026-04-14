package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.converters.ConverterUtils.base64Encode;
import static org.gbif.pipelines.core.converters.ConverterUtils.mapTerm;
import static org.gbif.pipelines.core.utils.ExtensionUtils.convertMoFFromVerbatim;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.gbif.dwc.terms.*;
import org.gbif.pipelines.core.parsers.temporal.StringToDateFunctions;
import org.gbif.pipelines.core.pojo.HumboldtJsonView;
import org.gbif.pipelines.core.pojo.MoFData;
import org.gbif.pipelines.core.utils.MediaSerDeser;
import org.gbif.pipelines.core.utils.TemporalConverter;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.event.EventHdfsRecord;
import org.gbif.pipelines.io.avro.event.EventType;
import org.gbif.pipelines.io.avro.event.ParentEventGbifId;

/** Utility class to convert interpreted and extended records into {@link EventHdfsRecord}. */
@Slf4j
@Builder
public class EventHdfsRecordConverter {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private final ExtendedRecord extendedRecord;
  private final IdentifierRecord identifierRecord;
  private final LocationRecord locationRecord;
  private final TemporalRecord temporalRecord;
  private final MetadataRecord metadataRecord;
  private final MultimediaRecord multimediaRecord;
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
   * Collects data from {@link SpecificRecordBase} instances into a {@link EventHdfsRecord}.
   *
   * @return a {@link EventHdfsRecord} instance based on the input records
   */
  public EventHdfsRecord convert() {
    EventHdfsRecord eventHdfsRecord = new EventHdfsRecord();
    eventHdfsRecord.setIssue(new ArrayList<>());

    // Order is important
    mapIdentifierRecord(eventHdfsRecord);
    mapEventCoreRecord(eventHdfsRecord);
    mapTemporalRecord(eventHdfsRecord);
    mapLocationRecord(eventHdfsRecord);
    mapMultimediaRecord(eventHdfsRecord);
    mapExtendedRecord(eventHdfsRecord);
    mapProjectIds(eventHdfsRecord);
    mapHumboldtRecord(eventHdfsRecord);
    mapMetadataRecord(eventHdfsRecord);
    mapMoFFromVerbatim(eventHdfsRecord);

    return eventHdfsRecord;
  }

  /**
   * Sets the lastInterpreted and lastParsed dates if the new value is greater that the existing one
   * or if it is not set.
   */
  private static void setCreatedIfGreater(EventHdfsRecord eventHdfsRecord, Long created) {
    if (Objects.nonNull(created)) {
      Long maxCreated =
          Math.max(
              created,
              Optional.ofNullable(eventHdfsRecord.getLastinterpreted()).orElse(Long.MIN_VALUE));
      eventHdfsRecord.setLastinterpreted(maxCreated);
      eventHdfsRecord.setLastparsed(maxCreated);
    }
  }

  /**
   * Adds the list of issues to the list of issues in the {@link EventHdfsRecord}.
   *
   * @param issueRecord record issues
   * @param eventHdfsRecord target object
   */
  private static void addNonTaxonIssues(IssueRecord issueRecord, EventHdfsRecord eventHdfsRecord) {
    if (issueRecord == null || issueRecord.getIssueList() == null) {
      return;
    }

    List<String> issues = issueRecord.getIssueList();
    eventHdfsRecord.getIssue().addAll(issues);
  }

  /**
   * Adds the list of issues to the list of issues in the {@link EventHdfsRecord}.
   *
   * @param issueRecord record issues
   * @param eventHdfsRecord target object
   */
  private static void addTaxonIssues(IssueRecord issueRecord, EventHdfsRecord eventHdfsRecord) {
    if (Objects.nonNull(issueRecord) && Objects.nonNull(issueRecord.getIssueList())) {
      List<String> currentIssues = eventHdfsRecord.getIssue();
      currentIssues.addAll(issueRecord.getIssueList());
      eventHdfsRecord.setIssue(currentIssues);
    }
  }

  /** Copies the {@link LocationRecord} data into the {@link EventHdfsRecord}. */
  private void mapLocationRecord(EventHdfsRecord eventHdfsRecord) {
    if (locationRecord == null) {
      return;
    }
    eventHdfsRecord.setCountrycode(locationRecord.getCountryCode());
    eventHdfsRecord.setContinent(locationRecord.getContinent());
    eventHdfsRecord.setDecimallatitude(locationRecord.getDecimalLatitude());
    eventHdfsRecord.setDecimallongitude(locationRecord.getDecimalLongitude());
    eventHdfsRecord.setCoordinateprecision(locationRecord.getCoordinatePrecision());
    eventHdfsRecord.setCoordinateuncertaintyinmeters(
        locationRecord.getCoordinateUncertaintyInMeters());
    eventHdfsRecord.setDepth(locationRecord.getDepth());
    eventHdfsRecord.setDepthaccuracy(locationRecord.getDepthAccuracy());
    eventHdfsRecord.setElevation(locationRecord.getElevation());
    eventHdfsRecord.setElevationaccuracy(locationRecord.getElevationAccuracy());
    if (Objects.nonNull(locationRecord.getMaximumDistanceAboveSurfaceInMeters())) {
      eventHdfsRecord.setMaximumdistanceabovesurfaceinmeters(
          locationRecord.getMaximumDistanceAboveSurfaceInMeters().toString());
    }
    if (Objects.nonNull(locationRecord.getMinimumDistanceAboveSurfaceInMeters())) {
      eventHdfsRecord.setMinimumdistanceabovesurfaceinmeters(
          locationRecord.getMinimumDistanceAboveSurfaceInMeters().toString());
    }
    eventHdfsRecord.setStateprovince(locationRecord.getStateProvince());
    eventHdfsRecord.setWaterbody(locationRecord.getWaterBody());
    eventHdfsRecord.setHascoordinate(locationRecord.getHasCoordinate());
    eventHdfsRecord.setHasgeospatialissues(locationRecord.getHasGeospatialIssue());
    eventHdfsRecord.setRepatriated(locationRecord.getRepatriated());
    eventHdfsRecord.setLocality(locationRecord.getLocality());
    eventHdfsRecord.setPublishingcountry(locationRecord.getPublishingCountry());
    Optional.ofNullable(locationRecord.getGadm())
        .ifPresent(
            g -> {
              eventHdfsRecord.setLevel0gid(g.getLevel0Gid());
              eventHdfsRecord.setLevel1gid(g.getLevel1Gid());
              eventHdfsRecord.setLevel2gid(g.getLevel2Gid());
              eventHdfsRecord.setLevel3gid(g.getLevel3Gid());
              eventHdfsRecord.setLevel0name(g.getLevel0Name());
              eventHdfsRecord.setLevel1name(g.getLevel1Name());
              eventHdfsRecord.setLevel2name(g.getLevel2Name());
              eventHdfsRecord.setLevel3name(g.getLevel3Name());
            });
    eventHdfsRecord.setDistancefromcentroidinmeters(
        locationRecord.getDistanceFromCentroidInMeters());
    eventHdfsRecord.setGbifregion(locationRecord.getGbifRegion());
    eventHdfsRecord.setPublishedbygbifregion(locationRecord.getPublishedByGbifRegion());
    eventHdfsRecord.setHighergeography(locationRecord.getHigherGeography());
    eventHdfsRecord.setGeoreferencedby(locationRecord.getGeoreferencedBy());

    setCreatedIfGreater(eventHdfsRecord, locationRecord.getCreated());
    addNonTaxonIssues(locationRecord.getIssues(), eventHdfsRecord);
  }

  private void mapProjectIds(EventHdfsRecord eventHdfsRecord) {
    Set<String> projectIds = new LinkedHashSet<>();

    if (metadataRecord != null) {
      projectIds.add(metadataRecord.getProjectId());
    }

    if (eventCoreRecord != null
        && eventCoreRecord.getCreated() != null
        && eventCoreRecord.getProjectID() != null) {
      projectIds.addAll(eventCoreRecord.getProjectID());
    }

    if (!projectIds.isEmpty()) {
      eventHdfsRecord.setProjectid(new ArrayList<>(projectIds));
    }
  }

  /** Copies the {@link MetadataRecord} data into the {@link EventHdfsRecord}. */
  private void mapMetadataRecord(EventHdfsRecord eventHdfsRecord) {
    if (metadataRecord == null) {
      return;
    }
    eventHdfsRecord.setCrawlid(metadataRecord.getCrawlId());
    eventHdfsRecord.setDatasetkey(metadataRecord.getDatasetKey());
    eventHdfsRecord.setDatasettitle(metadataRecord.getDatasetTitle());
    eventHdfsRecord.setInstallationkey(metadataRecord.getInstallationKey());
    eventHdfsRecord.setProtocol(metadataRecord.getProtocol());
    eventHdfsRecord.setNetworkkey(metadataRecord.getNetworkKeys());
    eventHdfsRecord.setPublisher(metadataRecord.getPublisherTitle());
    eventHdfsRecord.setPublishingorgkey(metadataRecord.getPublishingOrganizationKey());
    eventHdfsRecord.setLastcrawled(metadataRecord.getLastCrawled());
    eventHdfsRecord.setProgrammeacronym(metadataRecord.getProgrammeAcronym());
    eventHdfsRecord.setHostingorganizationkey(metadataRecord.getHostingOrganizationKey());

    if (eventHdfsRecord.getLicense() == null) {
      eventHdfsRecord.setLicense(metadataRecord.getLicense());
    }

    setCreatedIfGreater(eventHdfsRecord, metadataRecord.getCreated());
    addNonTaxonIssues(metadataRecord.getIssues(), eventHdfsRecord);
  }

  /** Copies the {@link TemporalRecord} data into the {@link EventHdfsRecord}. */
  private void mapTemporalRecord(EventHdfsRecord eventHdfsRecord) {
    if (temporalRecord == null) {
      return;
    }
    Optional.ofNullable(temporalRecord.getDateIdentified())
        .map(StringToDateFunctions.getStringToEarliestEpochSeconds(false))
        .ifPresent(eventHdfsRecord::setDateidentified);
    Optional.ofNullable(temporalRecord.getModified())
        .map(StringToDateFunctions.getStringToEarliestEpochSeconds(false))
        .ifPresent(eventHdfsRecord::setModified);
    eventHdfsRecord.setDay(temporalRecord.getDay());
    eventHdfsRecord.setMonth(temporalRecord.getMonth());
    eventHdfsRecord.setYear(temporalRecord.getYear());

    if (Objects.nonNull(temporalRecord.getStartDayOfYear())) {
      eventHdfsRecord.setStartdayofyear(temporalRecord.getStartDayOfYear());
    } else {
      eventHdfsRecord.setStartdayofyear(null);
    }

    if (Objects.nonNull(temporalRecord.getEndDayOfYear())) {
      eventHdfsRecord.setEnddayofyear(temporalRecord.getEndDayOfYear());
    } else {
      eventHdfsRecord.setEnddayofyear(null);
    }

    if (temporalRecord.getEventDate() != null
        && temporalRecord.getEventDate().getGte() != null
        && temporalRecord.getEventDate().getLte() != null) {
      Optional.ofNullable(temporalRecord.getEventDate())
          .ifPresent(
              eventDate -> {
                eventHdfsRecord.setEventdate(eventDate.getInterval());
                eventHdfsRecord.setEventdategte(
                    StringToDateFunctions.getStringToEarliestEpochSeconds(true)
                        .apply(eventDate.getGte()));
                eventHdfsRecord.setEventdatelte(
                    StringToDateFunctions.getStringToLatestEpochSeconds(true)
                        .apply(eventDate.getLte()));
              });
    } else {
      TemporalConverter.from(
              temporalRecord.getYear(), temporalRecord.getMonth(), temporalRecord.getDay())
          .ifPresent(
              eventDate -> {
                eventHdfsRecord.setEventdate(eventDate.toString());
                long instant =
                    ((LocalDate) eventDate)
                        .atStartOfDay(ZoneOffset.UTC)
                        .toInstant()
                        .getEpochSecond();
                eventHdfsRecord.setEventdategte(instant);
                eventHdfsRecord.setEventdatelte(instant);
              });
    }

    setCreatedIfGreater(eventHdfsRecord, temporalRecord.getCreated());
    addNonTaxonIssues(temporalRecord.getIssues(), eventHdfsRecord);
  }

  /** Copies the {@link IdentifierRecord} data into the {@link EventHdfsRecord}. */
  private void mapIdentifierRecord(EventHdfsRecord eventHdfsRecord) {
    if (identifierRecord == null) {
      return;
    }
    if (Objects.nonNull(identifierRecord.getInternalId())) {
      eventHdfsRecord.setGbifid(identifierRecord.getInternalId());
    }

    setCreatedIfGreater(eventHdfsRecord, identifierRecord.getFirstLoaded());
    addNonTaxonIssues(identifierRecord.getIssues(), eventHdfsRecord);
  }

  /** Copies the {@link ExtendedRecord} data into the {@link EventHdfsRecord}. */
  private void mapExtendedRecord(EventHdfsRecord eventHdfsRecord) {
    if (extendedRecord == null) {
      return;
    }

    extendedRecord.getCoreTerms().forEach((k, v) -> mapTerm(k, v, eventHdfsRecord));

    List<String> extensions =
        extendedRecord.getExtensions().entrySet().stream()
            .filter(e -> e.getValue() != null && !e.getValue().isEmpty())
            .map(Entry::getKey)
            .collect(Collectors.toList());
    eventHdfsRecord.setDwcaextension(extensions);
  }

  private void mapMoFFromVerbatim(EventHdfsRecord eventHdfsRecord) {
    MoFData moFData = convertMoFFromVerbatim(extendedRecord);
    if (!moFData.getMeasurementTypes().isEmpty()) {
      eventHdfsRecord.setMeasurementtype(new ArrayList<>(moFData.getMeasurementTypes()));
    }
    if (!moFData.getMeasurementTypeIDs().isEmpty()) {
      eventHdfsRecord.setMeasurementtypeid(new ArrayList<>(moFData.getMeasurementTypeIDs()));
    }
  }

  /** Copies the {@link EventCoreRecord} data into the {@link EventHdfsRecord}. */
  private void mapEventCoreRecord(EventHdfsRecord eventHdfsRecord) {
    if (eventCoreRecord != null && eventCoreRecord.getCreated() != null) {
      if (eventCoreRecord.getParentsLineage() != null) {
        eventHdfsRecord.setParenteventgbifid(
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
        eventHdfsRecord.setEventtype(
            EventType.newBuilder()
                .setConcept(eventCoreRecord.getEventType().getConcept())
                .setLineage(eventCoreRecord.getEventType().getLineage())
                .build());
      }

      eventHdfsRecord.setProjecttitle(eventCoreRecord.getProjectTitle());
      eventHdfsRecord.setProjectid(eventCoreRecord.getProjectID());
      eventHdfsRecord.setFundingattribution(eventCoreRecord.getFundingAttribution());
      eventHdfsRecord.setFundingattributionid(eventCoreRecord.getFundingAttributionID());
      eventHdfsRecord.setDatasetid(eventCoreRecord.getDatasetID());
      eventHdfsRecord.setDatasetname(eventCoreRecord.getDatasetName());
      eventHdfsRecord.setSamplingprotocol(eventCoreRecord.getSamplingProtocol());
      eventHdfsRecord.setSamplesizeunit(eventCoreRecord.getSampleSizeUnit());
      eventHdfsRecord.setSamplesizevalue(eventCoreRecord.getSampleSizeValue());
      eventHdfsRecord.setParenteventid(eventCoreRecord.getParentEventID());
      eventHdfsRecord.setReferences(eventCoreRecord.getReferences());
      eventHdfsRecord.setLicense(eventCoreRecord.getLicense());
      eventHdfsRecord.setLicense(eventCoreRecord.getLicense());
      eventHdfsRecord.setLocationid(eventCoreRecord.getLocationID());
    }
  }

  /**
   * Collects the {@link MultimediaRecord} mediaTypes data into the {@link
   * EventHdfsRecord#setMediatype(List)}.
   */
  private void mapMultimediaRecord(EventHdfsRecord eventHdfsRecord) {
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
    eventHdfsRecord.setExt_multimedia(
        MediaSerDeser.multimediaToJson(multimediaRecord.getMultimediaItems()));

    setCreatedIfGreater(eventHdfsRecord, multimediaRecord.getCreated());
    eventHdfsRecord.setMediatype(mediaTypes);

    addNonTaxonIssues(multimediaRecord.getIssues(), eventHdfsRecord);
  }

  private void mapHumboldtRecord(EventHdfsRecord eventHdfsRecord) {
    if (humboldtRecord == null || humboldtRecord.getCreated() == null) {
      return;
    }

    if (humboldtRecord.getHumboldtItems() != null) {

      Function<List<VocabularyConcept>, HumboldtJsonView.VocabularyList> convertVocabList =
          list -> {
            List<String> allConcepts =
                list.stream().map(VocabularyConcept::getConcept).collect(Collectors.toList());

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
                Map<String, Map<String, List<String>>> valuesAsList = new LinkedHashMap<>();

                r.stream()
                    .filter(v -> v.getChecklistKey() != null)
                    .forEach(
                        t -> {
                          Map<String, List<String>> values =
                              valuesAsList.computeIfAbsent(
                                  t.getChecklistKey(), k -> new LinkedHashMap<>());

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
                                      .collect(Collectors.toCollection(LinkedHashSet::new)));
                          values
                              .computeIfAbsent("issues", k -> new ArrayList<>())
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

      eventHdfsRecord.setExt_humboldt(base64Encode(MediaSerDeser.humboldtToJson(jsonViews)));
    }

    addNonTaxonIssues(humboldtRecord.getIssues(), eventHdfsRecord);

    // Add taxonomic issues from humboldtRecord
    humboldtRecord
        .getHumboldtItems()
        .forEach(
            h ->
                h.getTargetTaxonomicScope()
                    .forEach(ir -> addTaxonIssues(ir.getIssues(), eventHdfsRecord)));
  }
}
