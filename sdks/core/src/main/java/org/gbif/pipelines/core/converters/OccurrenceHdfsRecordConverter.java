package org.gbif.pipelines.core.converters;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.pipelines.core.utils.MediaSerDeserUtils;
import org.gbif.pipelines.core.utils.TemporalUtils;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.beanutils.PropertyUtils;

import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class to convert interpreted and extended records into {@link OccurrenceHdfsRecord}.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OccurrenceHdfsRecordConverter {

  //Registered converters
  private static Map<Class<? extends SpecificRecordBase>, BiConsumer<OccurrenceHdfsRecord,SpecificRecordBase>>
    converters;

  private static final TermFactory TERM_FACTORY =  TermFactory.instance();

  //Converters
  static {
    converters = new HashMap<>();
    converters.put(ExtendedRecord.class, extendedRecordMapper());
    converters.put(BasicRecord.class, basicRecordMapper());
    converters.put(LocationRecord.class, locationMapper());
    converters.put(TaxonRecord.class, taxonMapper());
    converters.put(TemporalRecord.class, temporalMapper());
    converters.put(MetadataRecord.class, metadataMapper());
    converters.put(MultimediaRecord.class, multimediaMapper());
    converters.put(TaggedValueRecord.class, taggedValueMapper());
  }

  //Converts a TemporalAccessor into Date
  private static final Function<TemporalAccessor, Date> TEMPORAL_TO_DATE =
    temporalAccessor -> {
      if (temporalAccessor instanceof ZonedDateTime) {
        return Date.from(((ZonedDateTime)temporalAccessor).toInstant());
      } else if (temporalAccessor instanceof LocalDateTime) {
        return Date.from(((LocalDateTime)temporalAccessor).toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof LocalDate) {
        return Date.from(((LocalDate)temporalAccessor).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof YearMonth) {
        return Date.from(((YearMonth)temporalAccessor).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else if (temporalAccessor instanceof Year) {
        return Date.from(((Year)temporalAccessor).atDay(1).atStartOfDay().toInstant(ZoneOffset.UTC));
      } else {
        return null;
      }
    };

  //Supported Date formats
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern(
          "[yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mmXXX][yyyy-MM-dd'T'HH:mm:ss.SSS XXX][yyyy-MM-dd'T'HH:mm:ss.SSSXXX]"
              + "[yyyy-MM-dd'T'HH:mm:ss.SSSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSSS][yyyy-MM-dd'T'HH:mm:ss.SSSS][yyyy-MM-dd'T'HH:mm:ss.SSS]"
              + "[yyyy-MM-dd'T'HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss XXX][yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mm:ss]"
              + "[yyyy-MM-dd'T'HH:mm][yyyy-MM-dd][yyyy-MM][yyyy]");

  //Converts a String into Date
  static final Function<String, Date> STRING_TO_DATE =
      dateAsString -> {
        if (Strings.isNullOrEmpty(dateAsString)) {
          return null;
        }

        boolean firstYear = false;
        if (dateAsString.startsWith("0000")) {
          firstYear = true;
          dateAsString = dateAsString.replaceFirst("0000", "1970");
        }

        try {
          // parse string
          TemporalAccessor temporalAccessor = FORMATTER.parseBest(dateAsString,
              ZonedDateTime::from,
              LocalDateTime::from,
              LocalDate::from,
              YearMonth::from,
              Year::from);

          Date date = TEMPORAL_TO_DATE.apply(temporalAccessor);

          if (date != null && firstYear) {
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTime(date);
            cal.set(Calendar.YEAR, 1);
            return cal.getTime();
          }

          return date;
        } catch (Exception ex) {
          return null;
        }
      };

  /**
   * Sets the lastInterpreted and lastParsed dates if the new value is greater that the existing one or if it is not set.
   */
  private static void setCreatedIfGreater(OccurrenceHdfsRecord hr, Long created) {
    if (Objects.nonNull(created)) {
      Long maxCreated = Math.max(created, Optional.ofNullable(hr.getLastinterpreted()).orElse(Long.MIN_VALUE));
      hr.setLastinterpreted(maxCreated);
      hr.setLastparsed(maxCreated);
    }
  }

  /**
   * Adds the list of issues to the list of issues in the {@link OccurrenceHdfsRecord}.
   * @param issueRecord record issues
   * @param hr target object
   */
  private static void addIssues(IssueRecord issueRecord, OccurrenceHdfsRecord hr) {
    if (Objects.nonNull(issueRecord) && Objects.nonNull(issueRecord.getIssueList())) {
      List<String> currentIssues =  hr.getIssue();
      currentIssues.addAll(issueRecord.getIssueList());
      hr.setIssue(currentIssues);
    }
  }

  /**
   * Copies the {@link LocationRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord,SpecificRecordBase> locationMapper() {
    return (hr, sr) -> {
      LocationRecord lr = (LocationRecord)sr;
      hr.setCountrycode(lr.getCountryCode());
      hr.setContinent(lr.getContinent());
      hr.setDecimallatitude(lr.getDecimalLatitude());
      hr.setDecimallongitude(lr.getDecimalLongitude());
      hr.setCoordinateprecision(lr.getCoordinatePrecision());
      hr.setCoordinateuncertaintyinmeters(lr.getCoordinateUncertaintyInMeters());
      hr.setDepth(lr.getDepth());
      hr.setDepthaccuracy(lr.getDepthAccuracy());
      hr.setElevation(lr.getElevation());
      hr.setElevationaccuracy(lr.getElevationAccuracy());
      if (Objects.nonNull(lr.getMaximumDistanceAboveSurfaceInMeters())) {
        hr.setMaximumdistanceabovesurfaceinmeters(lr.getMaximumDistanceAboveSurfaceInMeters().toString());
      }
      if (Objects.nonNull(lr.getMinimumDistanceAboveSurfaceInMeters())) {
        hr.setMinimumdistanceabovesurfaceinmeters(lr.getMinimumDistanceAboveSurfaceInMeters().toString());
      }
      hr.setStateprovince(lr.getStateProvince());
      hr.setWaterbody(lr.getWaterBody());
      hr.setHascoordinate(lr.getHasCoordinate());
      hr.setHasgeospatialissues(lr.getHasGeospatialIssue());
      hr.setRepatriated(lr.getRepatriated());
      hr.setLocality(lr.getLocality());
      hr.setPublishingcountry(lr.getPublishingCountry());
      hr.setLevel0gid(lr.getGadmLevel0Gid());
      hr.setLevel1gid(lr.getGadmLevel1Gid());
      hr.setLevel2gid(lr.getGadmLevel2Gid());
      hr.setLevel3gid(lr.getGadmLevel3Gid());
      hr.setLevel0name(lr.getGadmLevel0Name());
      hr.setLevel1name(lr.getGadmLevel1Name());
      hr.setLevel2name(lr.getGadmLevel2Name());
      hr.setLevel3name(lr.getGadmLevel3Name());

      setCreatedIfGreater(hr, lr.getCreated());
      addIssues(lr.getIssues(), hr);
    };
  }

  /**
   * Copies the {@link MetadataRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> metadataMapper() {
    return (hr, sr) -> {
      MetadataRecord mr = (MetadataRecord) sr;
      hr.setCrawlid(mr.getCrawlId());
      hr.setDatasetkey(mr.getDatasetKey());
      hr.setDatasetname(mr.getDatasetTitle());
      hr.setInstallationkey(mr.getInstallationKey());
      hr.setProtocol(mr.getProtocol());
      hr.setNetworkkey(mr.getNetworkKeys());
      hr.setPublisher(mr.getPublisherTitle());
      hr.setPublishingorgkey(mr.getPublishingOrganizationKey());
      hr.setLastcrawled(mr.getLastCrawled());
      hr.setProjectid(mr.getProjectId());
      hr.setProgrammeacronym(mr.getProgrammeAcronym());

      if (hr.getLicense() == null) {
        hr.setLicense(mr.getLicense());
      }

      setCreatedIfGreater(hr, mr.getCreated());
      addIssues(mr.getIssues(), hr);
    };
  }


  /**
   * Copies the {@link TemporalRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> temporalMapper() {
    return (hr, sr) -> {
      TemporalRecord tr = (TemporalRecord)sr;
      Optional.ofNullable(tr.getDateIdentified()).map(STRING_TO_DATE).ifPresent(date -> hr.setDateidentified(date.getTime()));
      Optional.ofNullable(tr.getModified()).map(STRING_TO_DATE).ifPresent(date -> hr.setModified(date.getTime()));
      hr.setDay(tr.getDay());
      hr.setMonth(tr.getMonth());
      hr.setYear(tr.getYear());

      if (Objects.nonNull(tr.getStartDayOfYear())) {
        hr.setStartdayofyear(tr.getStartDayOfYear().toString());
      } else {
        hr.setStartdayofyear(null);
      }


      if (Objects.nonNull(tr.getEndDayOfYear())) {
        hr.setEnddayofyear(tr.getEndDayOfYear().toString());
      } else {
        hr.setEnddayofyear(null);
      }

      if (tr.getEventDate() != null && tr.getEventDate().getGte() != null) {
        Optional.ofNullable(tr.getEventDate().getGte())
            .map(STRING_TO_DATE)
            .ifPresent(eventDate -> hr.setEventdate(eventDate.getTime()));
      } else {
        TemporalUtils.getTemporal(tr.getYear(), tr.getMonth(), tr.getDay())
            .map(TEMPORAL_TO_DATE)
            .ifPresent(eventDate -> hr.setEventdate(eventDate.getTime()));
      }

      setCreatedIfGreater(hr, tr.getCreated());
      addIssues(tr.getIssues(), hr);
    };
  }

  /**
   * Copies the {@link TaxonRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> taxonMapper() {
    return (hr, sr) -> {
      TaxonRecord tr = (TaxonRecord) sr;
      Optional.ofNullable(tr.getUsage()).ifPresent(x -> hr.setTaxonkey(x.getKey()));
      if (Objects.nonNull(tr.getClassification())) {
        tr.getClassification().forEach(rankedName -> {
          switch (rankedName.getRank()) {
            case KINGDOM:
              hr.setKingdom(rankedName.getName());
              hr.setKingdomkey(rankedName.getKey());
              break;
            case PHYLUM:
              hr.setPhylum(rankedName.getName());
              hr.setPhylumkey(rankedName.getKey());
              break;
            case CLASS:
              hr.setClass$(rankedName.getName());
              hr.setClasskey(rankedName.getKey());
              break;
            case ORDER:
              hr.setOrder(rankedName.getName());
              hr.setOrderkey(rankedName.getKey());
              break;
            case FAMILY:
              hr.setFamily(rankedName.getName());
              hr.setFamilykey(rankedName.getKey());
              break;
            case GENUS:
              hr.setGenus(rankedName.getName());
              hr.setGenuskey(rankedName.getKey());
              break;
            case SUBGENUS:
              hr.setSubgenus(rankedName.getName());
              hr.setSubgenuskey(rankedName.getKey());
              break;
            case SPECIES:
              hr.setSpecies(rankedName.getName());
              hr.setSpecieskey(rankedName.getKey());
              break;
            default:
              break;
          }
        });
      }

      if (Objects.nonNull(tr.getAcceptedUsage())) {
        hr.setAcceptedscientificname(tr.getAcceptedUsage().getName());
        hr.setAcceptednameusageid(tr.getAcceptedUsage().getKey().toString());
        if (Objects.nonNull(tr.getAcceptedUsage().getKey())) {
          hr.setAcceptedtaxonkey(tr.getAcceptedUsage().getKey());
        }
        Optional.ofNullable(tr.getAcceptedUsage().getRank()).ifPresent(r -> hr.setTaxonrank(r.name()));
      } else if (Objects.nonNull(tr.getUsage()) && tr.getUsage().getKey() != 0) {
        // if the acceptedUsage is null we use the usage as the accepted as longs as it's not incertidae sedis
        hr.setAcceptedtaxonkey(tr.getUsage().getKey());
        hr.setAcceptedscientificname(tr.getUsage().getName());
        hr.setAcceptednameusageid(tr.getUsage().getKey().toString());
      }

      if (Objects.nonNull(tr.getUsage())) {
        hr.setTaxonkey(tr.getUsage().getKey());
        hr.setScientificname(tr.getUsage().getName());
        Optional.ofNullable(tr.getUsage().getRank()).ifPresent(r -> hr.setTaxonrank(r.name()));
      }

      if (Objects.nonNull(tr.getUsageParsedName())) {
        hr.setGenericname(Objects.nonNull(tr.getUsageParsedName().getGenus())
                            ? tr.getUsageParsedName().getGenus()
                            : tr.getUsageParsedName().getUninomial());
        hr.setSpecificepithet(tr.getUsageParsedName().getSpecificEpithet());
        hr.setInfraspecificepithet(tr.getUsageParsedName().getInfraspecificEpithet());
      }

      Optional.ofNullable(tr.getDiagnostics())
          .map(Diagnostic::getStatus)
          .ifPresent(d -> hr.setTaxonomicstatus(d.name()));

      setCreatedIfGreater(hr, tr.getCreated());
      addIssues(tr.getIssues(), hr);
    };
  }

  /**
   * Copies the {@link BasicRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> basicRecordMapper() {
    return (hr, sr) -> {
      BasicRecord br = (BasicRecord)sr;
      if (Objects.nonNull(br.getGbifId())) {
        hr.setGbifid(br.getGbifId());
      }
      hr.setBasisofrecord(br.getBasisOfRecord());
      hr.setEstablishmentmeans(br.getEstablishmentMeans());
      hr.setIndividualcount(br.getIndividualCount());
      hr.setLifestage(br.getLifeStage());
      hr.setReferences(br.getReferences());
      hr.setSex(br.getSex());
      hr.setTypestatus(br.getTypeStatus());
      hr.setTypifiedname(br.getTypifiedName());
      hr.setOrganismquantity(br.getOrganismQuantity());
      hr.setOrganismquantitytype(br.getOrganismQuantityType());
      hr.setSamplesizeunit(br.getSampleSizeUnit());
      hr.setSamplesizevalue(br.getSampleSizeValue());
      hr.setRelativeorganismquantity(br.getRelativeOrganismQuantity());
      hr.setOccurrencestatus(br.getOccurrenceStatus());

      Optional.ofNullable(br.getRecordedByIds())
          .ifPresent(
              uis -> hr.setRecordedbyid(uis.stream()
                  .map(AgentIdentifier::getValue)
                  .collect(Collectors.toList()))
          );

      Optional.ofNullable(br.getIdentifiedByIds())
          .ifPresent(
              uis -> hr.setIdentifiedbyid(uis.stream()
                  .map(AgentIdentifier::getValue)
                  .collect(Collectors.toList()))
          );

      if (br.getLicense() != null && !License.UNSUPPORTED.name().equals(br.getLicense())
          && !License.UNSPECIFIED.name().equals(br.getLicense())) {
        hr.setLicense(br.getLicense());
      }

      setCreatedIfGreater(hr, br.getCreated());
      addIssues(br.getIssues(), hr);
    };
  }

  /**
   * The id (the <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and could only have been
   * used for "un-starring" a DWCA star record. However, we've exposed it as DcTerm.identifier for a long time in
   * our public API v1, so we continue to do this.the id (the <id> reference in the DWCA meta.xml) is an identifier
   * local to the DWCA, and could only have been used for "un-starring" a DWCA star record. However, we've exposed
   * it as DcTerm.identifier for a long time in our public API v1, so we continue to do this.
   */
  private static void setIdentifier(BasicRecord br, ExtendedRecord er, OccurrenceHdfsRecord hr) {

    String institutionCode = er.getCoreTerms().get(DwcTerm.institutionCode.qualifiedName());
    String collectionCode = er.getCoreTerms().get(DwcTerm.collectionCode.qualifiedName());
    String catalogNumber = er.getCoreTerms().get(DwcTerm.catalogNumber.qualifiedName());

    // id format following the convention of DwC (http://rs.tdwg.org/dwc/terms/#occurrenceID)
    String triplet = String.join(":", "urn:catalog", institutionCode, collectionCode, catalogNumber);
    String gbifId = Optional.ofNullable(br.getGbifId()).map(Object::toString).orElse("");

    String occId = er.getCoreTerms().get(DwcTerm.occurrenceID.qualifiedName());

    if (!br.getId().equals(gbifId) && (!Strings.isNullOrEmpty(occId) || !br.getId().equals(triplet))) {
      hr.setIdentifier(br.getId());
      hr.setVIdentifier(br.getId());
    }
  }

  /**
   * From a {@link Schema.Field} copies it value into a the {@link OccurrenceHdfsRecord} field using the recognized data type.
   * @param occurrenceHdfsRecord target record
   * @param avroField field to be copied
   * @param fieldName {@link OccurrenceHdfsRecord} field/property name
   * @param value field data/value
   */
  private static void setHdfsRecordField(OccurrenceHdfsRecord occurrenceHdfsRecord, Schema.Field avroField, String fieldName, String value) {
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


  /**
   * Copies the {@link ExtendedRecord} data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> extendedRecordMapper() {
    return (hr, sr) -> {
      ExtendedRecord er = (ExtendedRecord)sr;
      er.getCoreTerms().forEach((k, v) -> Optional.ofNullable(TERM_FACTORY.findTerm(k)).ifPresent(term -> {

        if (TermUtils.verbatimTerms().contains(term)) {
          Optional.ofNullable(verbatimSchemaField(term)).ifPresent(field -> {
            String verbatimField = "V" + field.name().substring(2, 3).toUpperCase() + field.name().substring(3);
            setHdfsRecordField(hr, field, verbatimField, v);
          });
        }

        if (!TermUtils.isInterpretedSourceTerm(term)) {
          Optional.ofNullable(interpretedSchemaField(term)).ifPresent(field -> {
            //Fields that were set by other mappers are ignored
            if (Objects.isNull(hr.get(field.name()))) {
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
              setHdfsRecordField(hr, field, interpretedFieldname, v);
            }
          });
        }
      }));
    };
  }

  /**
   * Collects data from {@link SpecificRecordBase} instances into a {@link OccurrenceHdfsRecord}.
   * @param records list of input records
   * @return a {@link OccurrenceHdfsRecord} instance based on the input records
   */
  public static OccurrenceHdfsRecord toOccurrenceHdfsRecord(SpecificRecordBase...records) {
    OccurrenceHdfsRecord occurrenceHdfsRecord = new OccurrenceHdfsRecord();
    occurrenceHdfsRecord.setIssue(new ArrayList<>());
    for (SpecificRecordBase record : records) {
      Optional.ofNullable(converters.get(record.getClass()))
        .ifPresent(consumer -> consumer.accept(occurrenceHdfsRecord, record));
    }

    // The id (the <id> reference in the DWCA meta.xml) is an identifier local to the DWCA, and could only have been
    // used for "un-starring" a DWCA star record. However, we've exposed it as DcTerm.identifier for a long time in
    // our public API v1, so we continue to do this.
    Optional<SpecificRecordBase> erOpt = Arrays.stream(records).filter(x -> x instanceof ExtendedRecord).findFirst();
    Optional<SpecificRecordBase> brOpt = Arrays.stream(records).filter(x -> x instanceof BasicRecord).findFirst();
    if (erOpt.isPresent() && brOpt.isPresent()) {
      setIdentifier((BasicRecord) brOpt.get(), (ExtendedRecord) erOpt.get(), occurrenceHdfsRecord);
    }

    return occurrenceHdfsRecord;
  }

  /**
   * Collects the {@link MultimediaRecord}  mediaTypes data into the {@link OccurrenceHdfsRecord#setMediatype(List)}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> multimediaMapper() {
    return (hr, sr) -> {
      MultimediaRecord mr = (MultimediaRecord)sr;
      // media types
      List<String> mediaTypes = mr.getMultimediaItems().stream()
        .filter(i -> !Strings.isNullOrEmpty(i.getType()))
        .map(Multimedia::getType)
        .map(TextNode::valueOf)
        .map(TextNode::asText)
        .collect(Collectors.toList());
      hr.setExtMultimedia(MediaSerDeserUtils.toJson(mr.getMultimediaItems()));

      setCreatedIfGreater(hr, mr.getCreated());
      hr.setMediatype(mediaTypes);
    };
  }


  /**
   * Collects the {@link TaggedValueRecord}  tagged values data into the {@link OccurrenceHdfsRecord}.
   */
  private static BiConsumer<OccurrenceHdfsRecord, SpecificRecordBase> taggedValueMapper() {
    return (hr, sr) -> {
      TaggedValueRecord tvr = (TaggedValueRecord)sr;
      Optional.ofNullable(tvr.getTaggedValues().get(GbifInternalTerm.projectId.qualifiedName())).ifPresent(hr::setProjectid);
      Optional.ofNullable(tvr.getTaggedValues().get(GbifInternalTerm.programmeAcronym.qualifiedName())).ifPresent(hr::setProgrammeacronym);
      Optional.ofNullable(tvr.getTaggedValues().get(GbifInternalTerm.collectionKey.qualifiedName())).ifPresent(hr::setCollectionkey);
      Optional.ofNullable(tvr.getTaggedValues().get(GbifInternalTerm.institutionKey.qualifiedName())).ifPresent(hr::setInstitutionkey);
    };
  }

  /**
   * Gets the {@link Schema.Field} associated to a verbatim term.
   */
  private static Schema.Field verbatimSchemaField(Term term) {
    return OccurrenceHdfsRecord.SCHEMA$.getField("v_" + term.simpleName().toLowerCase());
  }

  /**
   * Gets the {@link Schema.Field} associated to a interpreted term.
   */
  private static Schema.Field interpretedSchemaField(Term term) {
    return OccurrenceHdfsRecord.SCHEMA$.getField(HiveColumns.columnFor(term));
  }

}
