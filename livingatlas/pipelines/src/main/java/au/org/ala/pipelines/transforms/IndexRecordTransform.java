package au.org.ala.pipelines.transforms;

import static au.org.ala.pipelines.beam.ALAUUIDMintingPipeline.REMOVED_PREFIX_MARKER;
import static au.org.ala.pipelines.transforms.IndexValues.*;
import static org.apache.avro.Schema.Type.UNION;
import static org.gbif.pipelines.common.PipelinesVariables.Metrics.AVRO_TO_JSON_COUNT;

import au.org.ala.pipelines.common.SolrFieldSchema;
import au.org.ala.pipelines.interpreters.SensitiveDataInterpreter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.License;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.common.PipelinesException;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.io.avro.*;
import org.jetbrains.annotations.NotNull;

/**
 * A transform that creates IndexRecords which are used downstream to push data to a search index
 * (SOLR or ElasticSearch)
 */
@Slf4j
public class IndexRecordTransform implements Serializable, IndexFields {

  private static final long serialVersionUID = 1279313931024806169L;
  private static final TermFactory TERM_FACTORY = TermFactory.instance();
  public static final String ISSUES = "issues";
  public static final String CLASSS = "classs";
  public static final int YYYY_DD_MM_FORMAT_LENGTH = 10;
  public static final int YYYY_MM_DDTHH_mm_ss_Z_LENGTH = 22;
  public static final String RAW_PREFIX = "raw_";
  public static final String MULTIPLE_VALUES_DELIM = "\\|";
  public static final String YYYY_DD_MM_FORMAT = "yyyy-MM-dd";
  public static final String YYYY_MM_DDTHH_mm_ss_Z_FORMAT = "yyyy-MM-dd'T'HH:mmXXX";

  // Core
  @NonNull private TupleTag<ExtendedRecord> erTag;
  @NonNull private TupleTag<BasicRecord> brTag;
  @NonNull private TupleTag<TemporalRecord> trTag;
  @NonNull private TupleTag<LocationRecord> lrTag;

  private TupleTag<TaxonRecord> txrTag;
  @NonNull private TupleTag<ALATaxonRecord> atxrTag;

  private TupleTag<ALAAttributionRecord> aarTag;

  private TupleTag<MultimediaRecord> mrTag;

  @NonNull private TupleTag<ALAUUIDRecord> urTag;

  @NonNull private TupleTag<ImageRecord> isTag;

  @NonNull private TupleTag<TaxonProfile> tpTag;

  @NonNull private TupleTag<ALASensitivityRecord> srTag;

  @NonNull private TupleTag<EventCoreRecord> eventCoreTag;

  @NonNull private TupleTag<LocationRecord> eventLocationTag;

  @NonNull private TupleTag<TemporalRecord> eventTemporalTag;

  String datasetID;

  Long lastLoadDate;

  Long lastLoadProcessed;

  static Set<String> interpretedFields;

  static {
    interpretedFields = getAddedValues();
  }

  public static IndexRecordTransform create(
      TupleTag<ExtendedRecord> erTag,
      TupleTag<BasicRecord> brTag,
      TupleTag<TemporalRecord> trTag,
      TupleTag<LocationRecord> lrTag,
      TupleTag<TaxonRecord> txrTag,
      TupleTag<ALATaxonRecord> atxrTag,
      TupleTag<MultimediaRecord> mrTag,
      TupleTag<ALAAttributionRecord> aarTag,
      TupleTag<ALAUUIDRecord> urTag,
      TupleTag<ImageRecord> isTag,
      TupleTag<TaxonProfile> tpTag,
      TupleTag<ALASensitivityRecord> srTag,
      TupleTag<EventCoreRecord> eventCoreTag,
      TupleTag<LocationRecord> eventLocationTag,
      TupleTag<TemporalRecord> eventTemporalTag,
      String datasetID,
      Long lastLoadDate,
      Long lastLoadProcessed) {
    IndexRecordTransform t = new IndexRecordTransform();
    t.erTag = erTag;
    t.brTag = brTag;
    t.trTag = trTag;
    t.lrTag = lrTag;
    t.txrTag = txrTag;
    t.atxrTag = atxrTag;
    t.mrTag = mrTag;
    t.aarTag = aarTag;
    t.urTag = urTag;
    t.isTag = isTag;
    t.tpTag = tpTag;
    t.srTag = srTag;
    t.eventCoreTag = eventCoreTag;
    t.eventLocationTag = eventLocationTag;
    t.eventTemporalTag = eventTemporalTag;
    t.datasetID = datasetID;
    t.lastLoadDate = lastLoadDate;
    t.lastLoadProcessed = lastLoadProcessed;
    return t;
  }

  /**
   * Create a IndexRecord using the supplied records.
   *
   * @return IndexRecord
   */
  @NotNull
  public static IndexRecord createIndexRecord(
      BasicRecord br,
      TemporalRecord tr,
      LocationRecord lr,
      TaxonRecord txr,
      ALATaxonRecord atxr,
      ExtendedRecord er,
      ALAAttributionRecord aar,
      ALAUUIDRecord ur,
      ImageRecord isr,
      TaxonProfile tpr,
      ALASensitivityRecord sr,
      MultimediaRecord mr,
      EventCoreRecord ecr,
      LocationRecord elr,
      TemporalRecord etr,
      Long lastLoadDate,
      Long lastProcessedDate) {

    Set<String> skipKeys = new HashSet<>();
    skipKeys.add("id");
    skipKeys.add("created");
    skipKeys.add("text");
    skipKeys.add("name");
    skipKeys.add("coreRowType");
    skipKeys.add("coreTerms");
    skipKeys.add("extensions");
    skipKeys.add("usage");
    skipKeys.add("classification");
    skipKeys.add("eventDate");
    skipKeys.add("hasCoordinate");
    skipKeys.add("hasGeospatialIssue");
    skipKeys.add("gbifId");
    skipKeys.add("crawlId");
    skipKeys.add("networkKeys");
    skipKeys.add("protocol");
    skipKeys.add("issues");
    skipKeys.add("identifiedByIds"); // multi value field
    skipKeys.add("recordedByIds"); // multi value field
    skipKeys.add("machineTags");
    skipKeys.add("parentsLineage");
    skipKeys.add(
        "establishmentMeans"); // GBIF treats it as a JSON, but ALA needs a String which is defined
    skipKeys.add("identifiedByIds");
    skipKeys.add("recordedByIds");
    skipKeys.add(DwcTerm.typeStatus.simpleName());
    skipKeys.add(DwcTerm.recordedBy.simpleName());
    skipKeys.add(DwcTerm.identifiedBy.simpleName());
    skipKeys.add(DwcTerm.preparations.simpleName());
    skipKeys.add(DwcTerm.datasetID.simpleName());
    skipKeys.add(DwcTerm.datasetName.simpleName());
    skipKeys.add(DwcTerm.samplingProtocol.simpleName());
    skipKeys.add(DwcTerm.otherCatalogNumbers.simpleName());

    IndexRecord.Builder indexRecord = IndexRecord.newBuilder().setId(ur.getUuid());
    indexRecord.setBooleans(new HashMap<>());
    indexRecord.setStrings(new HashMap<>());
    indexRecord.setLongs(new HashMap<>());
    indexRecord.setInts(new HashMap<>());
    indexRecord.setDates(new HashMap<>());
    indexRecord.setDoubles(new HashMap<>());
    indexRecord.setMultiValues(new HashMap<>());
    List<String> assertions = new ArrayList<>();

    // add timestamps
    indexRecord.getDates().put(LAST_LOAD_DATE, lastLoadDate);
    indexRecord.getDates().put(LAST_PROCESSED_DATE, lastProcessedDate);

    // If a sensitive record, construct new versions of the data with adjustments
    boolean isSensitive = sr != null && sr.getIsSensitive() != null && sr.getIsSensitive();
    if (isSensitive) {
      Set<Term> sensitiveTerms =
          sr.getAltered().keySet().stream().map(TERM_FACTORY::findTerm).collect(Collectors.toSet());
      if (br != null) {
        br = BasicRecord.newBuilder(br).build();
        SensitiveDataInterpreter.applySensitivity(sensitiveTerms, sr, br);
      }
      if (tr != null) {
        tr = TemporalRecord.newBuilder(tr).build();
        SensitiveDataInterpreter.applySensitivity(sensitiveTerms, sr, tr);
      }
      if (lr != null) {
        lr = LocationRecord.newBuilder(lr).build();
        SensitiveDataInterpreter.applySensitivity(sensitiveTerms, sr, lr);
      }
      if (txr != null) {
        txr = TaxonRecord.newBuilder(txr).build();
        SensitiveDataInterpreter.applySensitivity(sensitiveTerms, sr, txr);
      }
      if (atxr != null) {
        atxr = ALATaxonRecord.newBuilder(atxr).build();
        SensitiveDataInterpreter.applySensitivity(sensitiveTerms, sr, atxr);
      }
      if (er != null) {
        er = ExtendedRecord.newBuilder(er).build();
        SensitiveDataInterpreter.applySensitivity(sensitiveTerms, sr, er);
      }
      if (aar != null) {
        aar = ALAAttributionRecord.newBuilder(aar).build();
        SensitiveDataInterpreter.applySensitivity(sensitiveTerms, sr, aar);
      }
    }

    addToIndexRecord(lr, indexRecord, skipKeys);
    addToIndexRecord(tr, indexRecord, skipKeys);
    addToIndexRecord(br, indexRecord, skipKeys);

    applyBasicRecord(br, indexRecord);

    // add event date
    applyTemporalRecord(tr, indexRecord);

    // GBIF taxonomy - add if available
    if (txr != null) {
      addGBIFTaxonomy(txr, indexRecord, assertions);
    }

    if (isSensitive) {
      indexRecord.getStrings().put(SENSITIVE, sr.getSensitive());
      if (sr.getDataGeneralizations() != null)
        indexRecord
            .getStrings()
            .put(DwcTerm.dataGeneralizations.simpleName(), sr.getDataGeneralizations());
      if (sr.getInformationWithheld() != null)
        indexRecord
            .getStrings()
            .put(DwcTerm.informationWithheld.simpleName(), sr.getInformationWithheld());
      if (sr.getGeneralisationInMetres() != null)
        indexRecord.getStrings().put(GENERALISATION_IN_METRES, sr.getGeneralisationInMetres());
      if (sr.getGeneralisationInMetres() != null)
        indexRecord
            .getStrings()
            .put(GENERALISATION_TO_APPLY_IN_METRES, sr.getGeneralisationInMetres());
      for (Map.Entry<String, String> entry : sr.getOriginal().entrySet()) {
        Term field = TERM_FACTORY.findTerm(entry.getKey());
        if (entry.getValue() != null) {
          indexRecord.getStrings().put(SENSITIVE_PREFIX + field.simpleName(), entry.getValue());
        }
      }
    }

    addGeo(indexRecord, lr);

    // ALA taxonomy & species groups - backwards compatible for EYA
    applyTaxonomy(atxr, skipKeys, indexRecord, assertions);

    // see https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/99
    boolean spatiallyValid = lr.getHasGeospatialIssue() == null || !lr.getHasGeospatialIssue();
    indexRecord.getBooleans().put(SPATIALLY_VALID, spatiallyValid);

    // see  https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/162
    if (ur.getFirstLoaded() != null) {
      indexRecord.getDates().put(FIRST_LOADED_DATE, ur.getFirstLoaded());
    }

    // Add legacy collectory fields
    applyAttribution(aar, indexRecord);

    // add image identifiers
    applyMultimedia(ur, isr, indexRecord);

    // add species lists
    addSpeciesListInfo(lr, tpr, indexRecord);

    List<String> temporalIssues = tr.getIssues().getIssueList();
    List<String> taxonomicIssues = atxr.getIssues().getIssueList();
    List<String> geospatialIssues = lr.getIssues().getIssueList();

    if (taxonomicIssues != null && !taxonomicIssues.isEmpty()) {
      indexRecord.getMultiValues().put(TAXONOMIC_ISSUES, temporalIssues);
    }

    if (geospatialIssues != null && !geospatialIssues.isEmpty()) {
      indexRecord.getMultiValues().put(GEOSPATIAL_ISSUES, geospatialIssues);
    }

    // add all to assertions
    assertions.addAll(temporalIssues);
    assertions.addAll(taxonomicIssues);
    assertions.addAll(geospatialIssues);

    if (mr != null) {
      assertions.addAll(mr.getIssues().getIssueList());
    }

    assertions.addAll(br.getIssues().getIssueList());

    if (sr != null) {
      assertions.addAll(sr.getIssues().getIssueList());
    }

    indexRecord.getMultiValues().put(ASSERTIONS, assertions);

    // Verbatim (Raw) data
    Map<String, String> raw = er.getCoreTerms();
    for (Map.Entry<String, String> entry : raw.entrySet()) {

      String key = entry.getKey();
      if (key.startsWith("http")) {
        key = key.substring(key.lastIndexOf("/") + 1);
      }

      // if we already have an interpreted value, prefix with raw_
      if (interpretedFields.contains(key)) {
        indexRecord.getStrings().put(RAW_PREFIX + key, entry.getValue());
      } else {
        if (key.endsWith(DwcTerm.dynamicProperties.simpleName())) {
          try {
            // index separate properties and the dynamicProperties
            // field as a string as it may not be parseable JSON
            indexRecord.getStrings().put(DwcTerm.dynamicProperties.simpleName(), entry.getValue());

            // attempt JSON parse - best effort service only, if this fails
            // we carry on indexing
            ObjectMapper om = new ObjectMapper();
            Map dynamicProperties = om.readValue(entry.getValue(), Map.class);

            // ensure the dynamic properties are maps of string, string to avoid serialisation
            // issues
            dynamicProperties.replaceAll((s, c) -> c != null ? c.toString() : "");

            indexRecord.setDynamicProperties(dynamicProperties);
          } catch (Exception e) {
            // NOP
          }
        } else {
          indexRecord.getStrings().put(key, entry.getValue());
        }
      }
    }

    Map<String, List<Map<String, String>>> extensions = er.getExtensions();

    List<Map<String, String>> identifications =
        extensions.get(Extension.IDENTIFICATION.getRowType());
    if (identifications != null && !identifications.isEmpty()) {
      // the flat SOLR schema will only allow for 1 identification per record
      Map<String, String> identification = identifications.get(0);
      addTermSafely(indexRecord, identification, DwcTerm.identificationID);
      addMultiValueTermSafely(indexRecord, identification, DwcTerm.identifiedBy);
      addTermSafely(indexRecord, identification, DwcTerm.identificationRemarks);
      addTermSafely(indexRecord, identification, DwcTerm.dateIdentified);
      addTermSafely(indexRecord, identification, DwcTerm.identificationQualifier);
    }

    List<Map<String, String>> loans = extensions.get(GGBN_TERMS_LOAN);
    if (loans != null && !loans.isEmpty()) {
      // the flat SOLR schema will only allow for 1 loan per record
      Map<String, String> loan = loans.get(0);
      addTermSafely(indexRecord, loan, LOAN_DESTINATION_TERM);
      addTermSafely(indexRecord, loan, LOAN_IDENTIFIER_TERM);
    }

    // apply inherited fields as required
    applyEventCore(indexRecord, ecr, skipKeys);
    applyEventLocation(indexRecord, elr, skipKeys);
    applyEventTemporal(indexRecord, etr, skipKeys);
    return indexRecord.build();
  }

  private static void applyMultimedia(
      ALAUUIDRecord ur, ImageRecord isr, IndexRecord.Builder indexRecord) {
    if (isr != null && isr.getImageItems() != null && !isr.getImageItems().isEmpty()) {

      Set<String> multimedia = new HashSet<>();
      Set<String> licenses = new HashSet<>();
      List<String> images = new ArrayList<>();
      List<String> videos = new ArrayList<>();
      List<String> sounds = new ArrayList<>();
      isr.getImageItems()
          .forEach(
              image -> {
                if (StringUtils.isNotEmpty(image.getLicense())) {
                  licenses.add(image.getLicense());
                }

                if (image.getFormat() != null) {
                  if (image.getFormat().startsWith("image")) {
                    multimedia.add(IMAGE);
                    images.add(image.getIdentifier());
                  }
                  if (image.getFormat().startsWith("audio")) {
                    multimedia.add(SOUND);
                    sounds.add(image.getIdentifier());
                  }
                  if (image.getFormat().startsWith("video")) {
                    multimedia.add(VIDEO);
                    videos.add(image.getIdentifier());
                  }
                }
              });

      if (!images.isEmpty()) {
        indexRecord.getStrings().put(IMAGE_ID, isr.getImageItems().get(0).getIdentifier());
        indexRecord
            .getMultiValues()
            .put(
                IMAGE_IDS,
                isr.getImageItems().stream()
                    .map(Image::getIdentifier)
                    .collect(Collectors.toList()));
      }
      if (!sounds.isEmpty()) {
        indexRecord
            .getMultiValues()
            .put(
                SOUND_IDS,
                isr.getImageItems().stream()
                    .map(Image::getIdentifier)
                    .collect(Collectors.toList()));
      }
      if (!videos.isEmpty()) {
        indexRecord
            .getMultiValues()
            .put(
                VIDEO_IDS,
                isr.getImageItems().stream()
                    .map(Image::getIdentifier)
                    .collect(Collectors.toList()));
      }

      List<MultimediaIndexRecord> mir =
          isr.getImageItems().stream()
              .map(imageItem -> convertToMultimediaRecord(ur.getUuid(), imageItem))
              .collect(Collectors.toList());
      indexRecord.setMultimedia(mir);

      if (!multimedia.isEmpty()) {
        List<String> distinctList = new ArrayList<>(multimedia);
        indexRecord.getMultiValues().put(MULTIMEDIA, distinctList);
      }

      if (!licenses.isEmpty()) {
        indexRecord.getMultiValues().put(MULTIMEDIA_LICENSE, new ArrayList<>(licenses));
      }
    }
  }

  private static void applyAttribution(ALAAttributionRecord aar, IndexRecord.Builder indexRecord) {
    if (aar != null) {
      // if the licence is null in the basic record (which is the record-level licence)
      // or licence is License.UNSPECIFIED in the basic record (licence provided at the record-level
      // is null)
      // then use the licence supplied by the collectory
      // see https://github.com/AtlasOfLivingAustralia/la-pipelines/issues/271
      if (indexRecord.getStrings().get(DcTerm.license.simpleName()) == null
          || indexRecord
              .getStrings()
              .get(DcTerm.license.simpleName())
              .equals(License.UNSPECIFIED.name())) {
        addIfNotEmpty(indexRecord, DcTerm.license.simpleName(), aar.getLicenseType());
      }

      addIfNotEmpty(indexRecord, DATA_RESOURCE_UID, aar.getDataResourceUid());
      addIfNotEmpty(indexRecord, DATA_RESOURCE_NAME, aar.getDataResourceName());
      addIfNotEmpty(indexRecord, DATA_PROVIDER_UID, aar.getDataProviderUid());
      addIfNotEmpty(indexRecord, DATA_PROVIDER_NAME, aar.getDataProviderName());
      addIfNotEmpty(indexRecord, INSTITUTION_UID, aar.getInstitutionUid());
      addIfNotEmpty(indexRecord, COLLECTION_UID, aar.getCollectionUid());
      addIfNotEmpty(indexRecord, INSTITUTION_NAME, aar.getInstitutionName());
      addIfNotEmpty(indexRecord, COLLECTION_NAME, aar.getCollectionName());
      addIfNotEmpty(indexRecord, PROVENANCE, aar.getProvenance());
      addIfNotEmpty(indexRecord, CONTENT_TYPES, aar.getContentTypes());
      indexRecord
          .getBooleans()
          .put(
              DEFAULT_VALUES_USED,
              aar.getHasDefaultValues() != null ? aar.getHasDefaultValues() : false);

      // add hub IDs
      if (aar.getHubMembership() != null && !aar.getHubMembership().isEmpty()) {
        indexRecord
            .getMultiValues()
            .put(
                DATA_HUB_UID,
                aar.getHubMembership().stream()
                    .map(EntityReference::getUid)
                    .collect(Collectors.toList()));
      }
    }
  }

  private static void applyTaxonomy(
      ALATaxonRecord atxr,
      Set<String> skipKeys,
      IndexRecord.Builder indexRecord,
      List<String> assertions) {
    if (atxr.getTaxonConceptID() != null) {
      List<Field> fields = atxr.getSchema().getFields();
      for (Field field : fields) {
        Object value = atxr.get(field.name());
        if (value != null
            && !field.name().equals(SPECIES_GROUP)
            && !field.name().equals(SPECIES_SUBGROUP)
            && !field.name().equals(TAXON_RANK)
            && !skipKeys.contains(field.name())) {

          if (field.name().equalsIgnoreCase(CLASSS)) {
            indexRecord.getStrings().put(DwcTerm.class_.simpleName(), value.toString());
          } else if (field.name().equalsIgnoreCase(ISSUES)) {
            assertions.add((String) value);
          } else {
            if (value instanceof Integer) {
              indexRecord.getInts().put(field.name(), (Integer) value);
            } else {
              indexRecord.getStrings().put(field.name(), value.toString());
            }
          }
        }

        if (atxr.getClasss() != null) {
          indexRecord.getStrings().put(DwcTerm.class_.simpleName(), atxr.getClasss());
        }

        if (atxr.getTaxonRank() != null) {
          indexRecord
              .getStrings()
              .put(DwcTerm.taxonRank.simpleName(), atxr.getTaxonRank().toLowerCase());
          if (atxr.getTaxonRankID() != null && atxr.getTaxonRankID() == SUBSPECIES_RANK_ID) {
            indexRecord.getStrings().put(SUBSPECIES, atxr.getScientificName());
            indexRecord.getStrings().put(SUBSPECIES_ID, atxr.getTaxonConceptID());
          }
        }
      }
      // legacy fields referenced in biocache-service code
      indexRecord.setTaxonID(atxr.getTaxonConceptID());
      indexRecord
          .getMultiValues()
          .put(
              SPECIES_GROUP,
              atxr.getSpeciesGroup().stream().distinct().collect(Collectors.toList()));
      indexRecord
          .getMultiValues()
          .put(
              SPECIES_SUBGROUP,
              atxr.getSpeciesSubgroup().stream().distinct().collect(Collectors.toList()));

      // required for EYA
      indexRecord
          .getStrings()
          .put(
              NAMES_AND_LSID,
              String.join(
                  "|",
                  atxr.getScientificName(),
                  atxr.getTaxonConceptID(),
                  StringUtils.trimToEmpty(atxr.getVernacularName()),
                  atxr.getKingdom(),
                  atxr.getFamily())); // is set to IGNORE in headerAttributes

      indexRecord
          .getStrings()
          .put(
              COMMON_NAME_AND_LSID,
              String.join(
                  "|",
                  StringUtils.trimToEmpty(atxr.getVernacularName()),
                  atxr.getScientificName(),
                  atxr.getTaxonConceptID(),
                  StringUtils.trimToEmpty(atxr.getVernacularName()),
                  atxr.getKingdom(),
                  atxr.getFamily())); // is set to IGNORE in headerAttribute
    }
  }

  private static void applyBasicRecord(BasicRecord br, IndexRecord.Builder indexRecord) {
    if (br != null) {
      addEstablishmentValueSafely(
          indexRecord, DwcTerm.establishmentMeans.simpleName(), br.getEstablishmentMeans());
      addTermWithAgentsSafely(
          indexRecord, DwcTerm.recordedByID.simpleName(), br.getRecordedByIds());
      addMultiValueTermSafely(indexRecord, DwcTerm.typeStatus.simpleName(), br.getTypeStatus());
      addMultiValueTermSafely(indexRecord, DwcTerm.recordedBy.simpleName(), br.getRecordedBy());
      addMultiValueTermSafely(indexRecord, DwcTerm.identifiedBy.simpleName(), br.getIdentifiedBy());
      addMultiValueTermSafely(indexRecord, DwcTerm.preparations.simpleName(), br.getPreparations());
      addMultiValueTermSafely(indexRecord, DwcTerm.datasetID.simpleName(), br.getDatasetID());
      addMultiValueTermSafely(indexRecord, DwcTerm.datasetName.simpleName(), br.getDatasetName());
      addMultiValueTermSafely(
          indexRecord, DwcTerm.samplingProtocol.simpleName(), br.getSamplingProtocol());
      addMultiValueTermSafely(
          indexRecord, DwcTerm.otherCatalogNumbers.simpleName(), br.getOtherCatalogNumbers());
    }
  }

  private static void applyTemporalRecord(TemporalRecord tr, IndexRecord.Builder indexRecord) {
    if (tr.getEventDate() != null) {

      Long date = parseInterpretedDate(tr.getEventDate().getGte());
      if (date != null) {
        indexRecord.getDates().put(DwcTerm.eventDate.simpleName(), date);
      }

      // eventDateEnd
      Long endDate = parseInterpretedDate(tr.getEventDate().getLte());
      if (endDate != null) {
        indexRecord.getDates().put(EVENT_DATE_END, endDate);
      }
    }

    if (tr.getDatePrecision() != null) {
      indexRecord.getStrings().put(DATE_PRECISION, tr.getDatePrecision());
    }

    if (tr.getYear() != null && tr.getYear() > 0) {
      indexRecord.getInts().put(DECADE, ((tr.getYear() / 10) * 10));

      // Added for backwards compatibility
      // see
      // https://github.com/AtlasOfLivingAustralia/biocache-store/blob/develop/src/main/scala/au/org/ala/biocache/index/IndexDAO.scala#L1077
      String occurrenceYear = tr.getYear() + "-01-01";
      try {
        long occurrenceYearTime =
            new SimpleDateFormat(YYYY_DD_MM_FORMAT).parse(occurrenceYear).getTime();
        indexRecord.getDates().put(OCCURRENCE_YEAR, occurrenceYearTime);
      } catch (ParseException ex) {
        // NOP
      }
    }
  }

  private static void applyEventTemporal(
      IndexRecord.Builder indexRecord, TemporalRecord ecr, Set<String> skipKeys) {
    if (ecr != null) {
      boolean hasTemporalInfo = indexRecord.getDates().get(DwcTerm.eventDate.simpleName()) != null;
      addToIndexRecord(ecr, indexRecord, skipKeys, false);
      if (!hasTemporalInfo) {
        applyTemporalRecord(ecr, indexRecord);
      }
    }
  }

  private static void applyEventLocation(
      IndexRecord.Builder indexRecord, LocationRecord ecr, Set<String> skipKeys) {
    if (ecr != null) {
      addToIndexRecord(ecr, indexRecord, skipKeys, false);
    }
  }

  private static void applyEventCore(
      IndexRecord.Builder indexRecord, EventCoreRecord ecr, Set<String> skipKeys) {
    if (ecr != null) {
      addToIndexRecord(ecr, indexRecord, skipKeys, false);
    }
  }

  /**
   * Event dates come through interpretation 3 formats 1) yyyy-MM-dd 2) yyyy-MM-ddTHH:mm:ssXXX e.g.
   * 2019-09-13T13:35+10:00 3) yyyy-MM-dd'T'HH:mm:ss.SSSZ e.g. 2022-06-15T00:02:11.396Z
   *
   * @param dateString
   * @return
   * @throws ParseException
   */
  private static Long parseInterpretedDate(String dateString) {

    if (dateString == null) {
      return null;
    }

    try {
      TemporalParser temporalParser = TemporalParser.create();
      OccurrenceParseResult<TemporalAccessor> r = temporalParser.parseRecordedDate(dateString);

      // FIXME  - im sure there is a better way to do this
      if (r.getPayload() instanceof LocalDateTime) {
        LocalDateTime ldt = ((LocalDateTime) r.getPayload());
        return ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
      } else if (r.getPayload() instanceof LocalDate) {
        LocalDate ldt = ((LocalDate) r.getPayload());
        ZoneId zoneId = ZoneId.systemDefault();
        return ldt.atStartOfDay(zoneId).toEpochSecond() * 1000;
      } else if (r.getPayload() instanceof OffsetDateTime) {
        OffsetDateTime ldt = ((OffsetDateTime) r.getPayload());
        return ldt.toInstant().toEpochMilli();
      } else if (r.getPayload() instanceof ZonedDateTime) {
        ZonedDateTime ldt = ((ZonedDateTime) r.getPayload());
        return ldt.toInstant().toEpochMilli();
      }
    } catch (Exception e) {
      log.error("Un-parsable date produced by downstream interpretation " + dateString);
    }
    return null;
  }

  private static void addTermWithAgentsSafely(
      IndexRecord.Builder indexRecord, String field, List<AgentIdentifier> agents) {
    if (agents != null && !agents.isEmpty()) {
      indexRecord
          .getMultiValues()
          .put(field, agents.stream().map(AgentIdentifier::getValue).collect(Collectors.toList()));
    }
  }

  private static void addMultiValueTermSafely(
      IndexRecord.Builder indexRecord, String indexField, List<String> values) {
    if (values != null && !values.isEmpty()) {
      List<String> multiValuedField =
          indexRecord.getMultiValues().getOrDefault(indexField, new ArrayList<>());
      multiValuedField.addAll(values);
      indexRecord.getMultiValues().put(indexField, multiValuedField);
    }
  }

  private static void addEstablishmentValueSafely(
      IndexRecord.Builder indexRecord, String field, VocabularyConcept establishmentMeans) {
    if (establishmentMeans != null) {
      indexRecord.getStrings().put(field, establishmentMeans.getConcept());
    }
  }

  private static void addTermSafely(
      IndexRecord.Builder indexRecord, Map<String, String> extension, DwcTerm dwcTerm) {
    String termValue = extension.get(dwcTerm.name());
    if (isNotBlank(termValue)) {
      indexRecord.getStrings().put(dwcTerm.simpleName(), termValue);
    }
  }

  private static void addMultiValueTermSafely(
      IndexRecord.Builder indexRecord, Map<String, String> extension, DwcTerm dwcTerm) {
    String termValue = extension.get(dwcTerm.name());
    if (isNotBlank(termValue)) {
      List<String> multiValuedField =
          indexRecord.getMultiValues().getOrDefault(dwcTerm.simpleName(), new ArrayList<>());
      multiValuedField.add(termValue);
      indexRecord.getMultiValues().put(dwcTerm.simpleName(), multiValuedField);
    }
  }

  private static void addTermSafely(
      IndexRecord.Builder indexRecord, Map<String, String> extension, String dwcTerm) {
    String termValue = extension.get(dwcTerm);
    if (isNotBlank(termValue)) {
      String termToUse = dwcTerm;
      if (dwcTerm.startsWith("http")) {
        termToUse = dwcTerm.substring(dwcTerm.lastIndexOf("/") + 1);
      }
      indexRecord.getStrings().put(termToUse, termValue);
    }
  }

  public static Set<String> getAddedValues() {
    return ImmutableSet.<String>builder()
        .addAll(
            LocationRecord.getClassSchema().getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList()))
        .addAll(
            ALAAttributionRecord.getClassSchema().getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList()))
        .addAll(
            ALATaxonRecord.getClassSchema().getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList()))
        .addAll(
            BasicRecord.getClassSchema().getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList()))
        .addAll(
            TemporalRecord.getClassSchema().getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList()))
        .addAll(
            ALASensitivityRecord.getClassSchema().getFields().stream()
                .map(Field::name)
                .collect(Collectors.toList()))
        .add(DwcTerm.class_.simpleName())
        .add(DwcTerm.geodeticDatum.simpleName())
        .add(DwcTerm.associatedOccurrences.simpleName())
        .build();
  }

  private static void addSpeciesListInfo(
      LocationRecord lr, TaxonProfile tpr, IndexRecord.Builder indexRecord) {

    if (tpr == null || tpr.getSpeciesListID() == null || tpr.getSpeciesListID().isEmpty()) {
      return;
    }

    indexRecord.getMultiValues().put(SPECIES_LIST_UID, tpr.getSpeciesListID());

    // CONSERVATION STATUS
    String stateProvince = lr.getStateProvince();
    String country = lr.getCountry();

    // index conservation status
    List<ConservationStatus> conservationStatuses = tpr.getConservationStatuses();
    for (ConservationStatus conservationStatus : conservationStatuses) {
      if (conservationStatus.getRegion() != null) {
        if (conservationStatus.getRegion().equalsIgnoreCase(stateProvince)) {

          if (isNotBlank(conservationStatus.getSourceStatus())) {
            indexRecord
                .getStrings()
                .put(RAW_STATE_CONSERVATION, conservationStatus.getSourceStatus());
          }
          if (isNotBlank(conservationStatus.getStatus())) {
            indexRecord.getStrings().put(STATE_CONSERVATION, conservationStatus.getStatus());
          }
        }
        if (conservationStatus.getRegion().equalsIgnoreCase(country)) {
          if (isNotBlank(conservationStatus.getStatus())) {
            indexRecord.getStrings().put(COUNTRY_CONSERVATION, conservationStatus.getStatus());
          }
        }
      }
    }

    // index invasive status
    List<InvasiveStatus> invasiveStatuses = tpr.getInvasiveStatuses();
    for (InvasiveStatus invasiveStatus : invasiveStatuses) {
      if (invasiveStatus.getRegion() != null) {
        if (invasiveStatus.getRegion().equalsIgnoreCase(stateProvince)) {
          indexRecord.getStrings().put(STATE_INVASIVE, INVASIVE);
        }
        if (invasiveStatus.getRegion().equalsIgnoreCase(country)) {
          indexRecord.getStrings().put(COUNTRY_INVASIVE, INVASIVE);
        }
      }
    }
  }

  private static MultimediaIndexRecord convertToMultimediaRecord(String uuid, Image image) {
    return MultimediaIndexRecord.newBuilder()
        .setId(uuid)
        .setAudience(image.getAudience())
        .setContributor(image.getContributor())
        .setCreated(image.getCreated())
        .setCreator(image.getCreator())
        .setFormat(image.getFormat())
        .setDescription(image.getDescription())
        .setTitle(image.getTitle())
        .setIdentifier(image.getIdentifier())
        .setLicense(image.getLicense())
        .setPublisher(image.getPublisher())
        .setRights(image.getRights())
        .setRightsHolder(image.getRightsHolder())
        .setReferences(image.getReferences())
        .build();
  }

  private static void addGBIFTaxonomy(
      TaxonRecord txr, IndexRecord.Builder indexRecord, List<String> assertions) {
    // add the classification
    List<RankedName> taxonomy = txr.getClassification();
    for (RankedName entry : taxonomy) {
      indexRecord
          .getInts()
          .put("gbif_s_" + entry.getRank().toString().toLowerCase() + "_id", entry.getKey());
      indexRecord
          .getStrings()
          .put("gbif_s_" + entry.getRank().toString().toLowerCase(), entry.getName());
    }

    indexRecord.getStrings().put("gbif_s_rank", txr.getAcceptedUsage().getRank().toString());
    indexRecord.getStrings().put("gbif_s_scientificName", txr.getAcceptedUsage().getName());

    IssueRecord taxonomicIssues = txr.getIssues();
    assertions.addAll(taxonomicIssues.getIssueList());
  }

  public ParDo.SingleOutput<KV<String, CoGbkResult>, IndexRecord> converter() {

    DoFn<KV<String, CoGbkResult>, IndexRecord> fn =
        new DoFn<KV<String, CoGbkResult>, IndexRecord>() {

          private final Counter counter =
              Metrics.counter(IndexRecordTransform.class, AVRO_TO_JSON_COUNT);

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            // ALA specific
            ALAUUIDRecord ur = v.getOnly(urTag, null);

            if (ur != null && !ur.getId().startsWith(REMOVED_PREFIX_MARKER)) {

              // Core
              ExtendedRecord er = v.getOnly(erTag, ExtendedRecord.newBuilder().setId(k).build());
              BasicRecord br = v.getOnly(brTag, BasicRecord.newBuilder().setId(k).build());
              TemporalRecord tr = v.getOnly(trTag, TemporalRecord.newBuilder().setId(k).build());
              LocationRecord lr = v.getOnly(lrTag, LocationRecord.newBuilder().setId(k).build());
              TaxonRecord txr = null;

              if (txrTag != null) {
                txr = v.getOnly(txrTag, TaxonRecord.newBuilder().setId(k).build());
              }

              ALATaxonRecord atxr =
                  v.getOnly(atxrTag, ALATaxonRecord.newBuilder().setId(k).build());

              ALAAttributionRecord aar =
                  v.getOnly(aarTag, ALAAttributionRecord.newBuilder().setId(k).build());

              ImageRecord isr = null;
              if (isTag != null) {
                isr = v.getOnly(isTag, ImageRecord.newBuilder().setId(k).build());
              }

              TaxonProfile tpr = null;
              if (tpTag != null) {
                tpr = v.getOnly(tpTag, TaxonProfile.newBuilder().setId(k).build());
              }

              ALASensitivityRecord sr = null;
              if (srTag != null) {
                sr = v.getOnly(srTag, null);
              }

              MultimediaRecord mr = null;
              if (srTag != null) {
                mr = v.getOnly(mrTag, null);
              }

              EventCoreRecord ecr = v.getOnly(eventCoreTag, null);
              LocationRecord elr = v.getOnly(eventLocationTag, null);
              TemporalRecord etr = v.getOnly(eventTemporalTag, null);

              if (aar != null && aar.getDataResourceUid() != null) {
                IndexRecord doc =
                    createIndexRecord(
                        br,
                        tr,
                        lr,
                        txr,
                        atxr,
                        er,
                        aar,
                        ur,
                        isr,
                        tpr,
                        sr,
                        mr,
                        ecr,
                        elr,
                        etr,
                        lastLoadDate,
                        lastLoadProcessed);
                c.output(doc);
                counter.inc();
              } else {
                if (aar == null) {
                  throw new PipelinesException("AAR missing for record ID " + ur.getId());
                } else {
                  throw new PipelinesException(
                      "AAR is present, but data resource UID is null for"
                          + " ur.getId():"
                          + ur.getId()
                          + " ur.getUuid():"
                          + ur.getUuid()
                          + " aar.getId(): "
                          + aar.getId());
                }
              }
            } else {
              if (ur != null && !ur.getId().startsWith(REMOVED_PREFIX_MARKER)) {
                log.error("UUID missing and ER empty");
                throw new PipelinesException("UUID missing and ER empty");
              }
            }
          }
        };

    return ParDo.of(fn);
  }

  static void addIfNotEmpty(IndexRecord.Builder doc, String fieldName, String value) {
    if (StringUtils.isNotEmpty(value)) {
      doc.getStrings().put(fieldName, value);
    }
  }

  static void addIfNotEmpty(IndexRecord.Builder doc, String fieldName, List<String> values) {
    if (values != null && !values.isEmpty()) {
      doc.getMultiValues().put(fieldName, values);
    }
  }

  static void addGeo(IndexRecord.Builder doc, LocationRecord lr) {

    if (lr.getHasCoordinate() == null
        || lr.getHasCoordinate()
        || lr.getDecimalLatitude() == null
        || lr.getDecimalLongitude() == null) return;

    Double lat = lr.getDecimalLatitude();
    Double lon = lr.getDecimalLongitude();

    String latlon = "";
    // ensure that the lat longs are in the required range before
    if (lat <= 90 && lat >= -90d && lon <= 180 && lon >= -180d) {
      // https://lucene.apache.org/solr/guide/7_0/spatial-search.html#indexing-points
      latlon = lat + "," + lon; // required format for indexing geodetic points in SOLR
      doc.setLatLng(latlon);
    } else {
      return;
    }

    doc.getStrings().put(DwcTerm.geodeticDatum.simpleName(), PIPELINES_GEODETIC_DATUM);
    doc.getStrings().put(LAT_LONG, latlon); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(POINT_1, getLatLongString(lat, lon, "#")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(POINT_0_1, getLatLongString(lat, lon, "#.#")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            POINT_0_01, getLatLongString(lat, lon, "#.##")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            POINT_0_02,
            getLatLongStringStep(lat, lon, "#.##", 0.02)); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            POINT_0_001,
            getLatLongString(lat, lon, "#.###")); // is set to IGNORE in headerAttributes
    doc.getStrings()
        .put(
            POINT_0_0001,
            getLatLongString(lat, lon, "#.####")); // is set to IGNORE in headerAttributes
  }

  static String getLatLongStringStep(Double lat, Double lon, String format, Double step) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(Math.round(lat / step) * step)
        + ","
        + df.format(Math.round(lon / step) * step);
  }

  /** Returns a lat,long string expression formatted to the supplied Double format */
  static String getLatLongString(Double lat, Double lon, String format) {
    DecimalFormat df = new DecimalFormat(format);
    // By some "strange" decision the default rounding model is HALF_EVEN
    df.setRoundingMode(java.math.RoundingMode.HALF_UP);
    return df.format(lat) + "," + df.format(lon);
  }

  static void addToIndexRecord(
      SpecificRecordBase record, IndexRecord.Builder builder, Set<String> skipKeys) {
    addToIndexRecord(record, builder, skipKeys, true);
  }

  static void addToIndexRecord(
      SpecificRecordBase record,
      IndexRecord.Builder builder,
      Set<String> skipKeys,
      boolean overwrite) {

    record.getSchema().getFields().stream()
        .filter(n -> !skipKeys.contains(n.name()))
        .forEach(
            f ->
                Optional.ofNullable(record.get(f.pos()))
                    .ifPresent(
                        r -> {
                          Schema schema = f.schema();
                          Optional<Schema.Type> type =
                              schema.getType() == UNION
                                  ? schema.getTypes().stream()
                                      .filter(t -> t.getType() != Schema.Type.NULL)
                                      .findFirst()
                                      .map(Schema::getType)
                                  : Optional.of(schema.getType());
                          type.ifPresent(
                              t -> {
                                switch (t) {
                                  case BOOLEAN:
                                    if (overwrite || builder.getBooleans().get(f.name()) == null) {
                                      builder.getBooleans().put(f.name(), (Boolean) r);
                                    }
                                    break;
                                  case FLOAT:
                                  case DOUBLE:
                                    if (overwrite || builder.getDoubles().get(f.name()) == null) {
                                      builder.getDoubles().put(f.name(), (Double) r);
                                    }
                                    break;
                                  case INT:
                                    if (overwrite || builder.getInts().get(f.name()) == null) {
                                      builder.getInts().put(f.name(), (Integer) r);
                                    }
                                    break;
                                  case LONG:
                                    if (overwrite || builder.getLongs().get(f.name()) == null) {
                                      builder.getLongs().put(f.name(), (Long) r);
                                    }
                                    break;
                                  case ARRAY:
                                    if (overwrite
                                        || builder.getMultiValues().get(f.name()) == null) {
                                      builder.getMultiValues().put(f.name(), (List) r);
                                    }
                                    break;
                                  default:
                                    if (overwrite || builder.getStrings().get(f.name()) == null) {
                                      builder.getStrings().put(f.name(), r.toString());
                                    }
                                    break;
                                }
                              });
                        }));
  }

  private static boolean startsWithPrefix(List<String> dynamicFieldPrefixes, String value) {
    for (String prefix : dynamicFieldPrefixes) {
      if (value.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  public static void addStringSafely(SolrInputDocument doc, String key, String value) {
    // current limitation on SOLR string field size
    if (value.getBytes().length < 32765) {
      doc.addField(key, value);
    }
  }

  public static SolrInputDocument convertIndexRecordToSolrDoc(
      IndexRecord indexRecord,
      Map<String, SolrFieldSchema> schemaFields,
      List<String> dynamicFieldPrefixes) {

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(ID, indexRecord.getId());

    // keep track of added dynamic properties
    for (Map.Entry<String, String> s : indexRecord.getStrings().entrySet()) {
      if (schemaFields.containsKey(s.getKey())
          || startsWithPrefix(dynamicFieldPrefixes, s.getKey())) {
        addStringSafely(doc, s.getKey(), s.getValue());
      } else {
        // clean up field name before adding
        String key = s.getKey().replaceAll("[^A-Za-z0-9]", "_");
        if (StringUtils.isNotEmpty(key)
            && doc.getFieldValue(DYNAMIC_PROPERTIES_PREFIX + key) == null) {
          SolrFieldSchema fieldSchema = schemaFields.get(DYNAMIC_PROPERTIES_PREFIX + key);
          if ((fieldSchema != null) && (fieldSchema.type != null)) {
            if (fieldSchema.multiple) {
              doc.addField(
                  DYNAMIC_PROPERTIES_PREFIX + key, s.getValue().split(MULTIPLE_VALUES_DELIM));
            } else {
              switch (fieldSchema.type) {
                case BOOLEAN:
                  doc.addField(DYNAMIC_PROPERTIES_PREFIX + key, Boolean.valueOf(s.getValue()));
                  break;
                case DATE:
                  try {
                    Date date = null;
                    if ((s.getValue() != null)
                        && (s.getValue().length() == YYYY_MM_DDTHH_mm_ss_Z_LENGTH)) {
                      SimpleDateFormat sdf = new SimpleDateFormat(YYYY_MM_DDTHH_mm_ss_Z_FORMAT);
                      date = sdf.parse(s.getValue());
                    }
                    if ((s.getValue() != null)
                        && (s.getValue().length() == YYYY_DD_MM_FORMAT_LENGTH)) {
                      SimpleDateFormat sdf = new SimpleDateFormat(YYYY_DD_MM_FORMAT);
                      date = sdf.parse(s.getValue());
                    }
                    doc.addField(DYNAMIC_PROPERTIES_PREFIX + key, date);
                  } catch (ParseException e) {
                    log.error("Cannot parse date " + s.getValue());
                  }
                  break;
                case DOUBLE:
                  doc.addField(DYNAMIC_PROPERTIES_PREFIX + key, Double.valueOf(s.getValue()));
                  break;
                case FLOAT:
                  doc.addField(DYNAMIC_PROPERTIES_PREFIX + key, Float.valueOf(s.getValue()));
                  break;
                case INT:
                  doc.addField(DYNAMIC_PROPERTIES_PREFIX + key, Integer.valueOf(s.getValue()));
                  break;
                case LONG:
                  doc.addField(DYNAMIC_PROPERTIES_PREFIX + key, Long.valueOf(s.getValue()));
                  break;
                case STRING:
                  addStringSafely(doc, DYNAMIC_PROPERTIES_PREFIX + key, s.getValue());
                  break;
              }
            }
          } else {
            addStringSafely(doc, DYNAMIC_PROPERTIES_PREFIX + key, s.getValue());
          }
        }
      }
    }

    // doubles
    for (Map.Entry<String, Double> s : indexRecord.getDoubles().entrySet()) {
      doc.addField(s.getKey(), s.getValue());
    }

    // integers
    for (Map.Entry<String, Integer> s : indexRecord.getInts().entrySet()) {
      doc.addField(s.getKey(), s.getValue());
    }

    // longs
    for (Map.Entry<String, Long> s : indexRecord.getLongs().entrySet()) {
      doc.addField(s.getKey(), s.getValue());
    }

    // dates
    for (Map.Entry<String, Long> s : indexRecord.getDates().entrySet()) {
      doc.addField(s.getKey(), new Date(s.getValue()));
    }

    // booleans
    for (Map.Entry<String, Boolean> s : indexRecord.getBooleans().entrySet()) {
      doc.addField(s.getKey(), s.getValue());
    }

    // multi-value fields
    for (Map.Entry<String, List<String>> s : indexRecord.getMultiValues().entrySet()) {
      for (String value : s.getValue()) {
        addStringSafely(doc, s.getKey(), value);
      }
    }

    // dynamic properties
    if (indexRecord.getDynamicProperties() != null
        && !indexRecord.getDynamicProperties().isEmpty()) {
      for (Map.Entry<String, String> entry : indexRecord.getDynamicProperties().entrySet()) {
        if (StringUtils.isNotEmpty(entry.getValue())) {
          String key = entry.getKey().replaceAll("[^A-Za-z0-9]", "_");
          if (StringUtils.isNotEmpty(key)
              && doc.getFieldValue(DYNAMIC_PROPERTIES_PREFIX + key) == null) {
            addStringSafely(doc, DYNAMIC_PROPERTIES_PREFIX + key, entry.getValue());
          }
        }
      }
    }

    return doc;
  }

  private static boolean isNotBlank(String s) {
    return s != null && !s.trim().isEmpty();
  }
}
