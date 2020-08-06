package org.gbif.pipelines.core.converters;

import static org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter.STRING_TO_DATE;
import static org.gbif.pipelines.core.converters.OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord;
import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.core.utils.MediaSerDeserUtils;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.Authorship;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.NamePart;
import org.gbif.pipelines.io.avro.NameType;
import org.gbif.pipelines.io.avro.Nomenclature;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.ParsedName;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.State;
import org.gbif.pipelines.io.avro.TaggedValueRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.junit.Assert;
import org.junit.Test;

public class OccurrenceHdfsRecordConverterTest {

  @Test
  public void extendedRecordMapperTest() {
    // State
    Map<String, String> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.verbatimDepth.simpleName(), "1.0");
    coreTerms.put(DwcTerm.collectionCode.simpleName(), "C1");
    coreTerms.put(DwcTerm.institutionCode.simpleName(), "I1");
    coreTerms.put(DwcTerm.catalogNumber.simpleName(), "CN1");
    coreTerms.put(DwcTerm.class_.simpleName(), "classs");
    coreTerms.put(DcTerm.format.simpleName(), "format");
    coreTerms.put(DwcTerm.order.simpleName(), "order");
    coreTerms.put(DwcTerm.group.simpleName(), "group");
    coreTerms.put(DcTerm.date.simpleName(), "26/06/2019");
    coreTerms.put(
        DwcTerm.basisOfRecord.simpleName(), BasisOfRecord.HUMAN_OBSERVATION.name().toLowerCase());
    coreTerms.put(DwcTerm.lifeStage.simpleName(), "adultss");
    coreTerms.put(DwcTerm.sampleSizeUnit.simpleName(), "unit");
    coreTerms.put(DwcTerm.sampleSizeValue.simpleName(), "value");
    coreTerms.put(DwcTerm.organismQuantity.simpleName(), "quantity");
    coreTerms.put(DwcTerm.organismQuantityType.simpleName(), "type");
    coreTerms.put(DwcTerm.recordedBy.simpleName(), "recordedBy");
    coreTerms.put(DwcTerm.identifiedBy.simpleName(), "identifiedBy");
    coreTerms.put(GbifTerm.identifiedByID.simpleName(), "13123|21312");
    coreTerms.put(GbifTerm.recordedByID.simpleName(), "53453|5785");
    coreTerms.put(DwcTerm.occurrenceStatus.simpleName(), OccurrenceStatus.ABSENT.name());
    coreTerms.put(DwcTerm.individualCount.simpleName(), "0");

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId("1").setCoreTerms(coreTerms).build();

    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder().setId("1").setLicense(License.CC_BY_4_0.name()).build();

    List<AgentIdentifier> agentIds =
        Collections.singletonList(
            AgentIdentifier.newBuilder()
                .setType(AgentIdentifierType.OTHER.name())
                .setValue("13123")
                .build());

    BasicRecord basicRecord =
        BasicRecord.newBuilder()
            .setId("1")
            .setCreated(1L)
            .setLicense(License.CC0_1_0.name())
            .setIdentifiedByIds(agentIds)
            .setRecordedByIds(agentIds)
            .setIndividualCount(0)
            .setBasisOfRecord(BasisOfRecord.HUMAN_OBSERVATION.name())
            .setOccurrenceStatus(OccurrenceStatus.ABSENT.name())
            .build();

    List<RankedName> classification = new ArrayList<>();
    classification.add(RankedName.newBuilder().setName("CLASS").setRank(Rank.CLASS).build());
    classification.add(RankedName.newBuilder().setName("ORDER").setRank(Rank.ORDER).build());
    TaxonRecord taxonRecord =
        TaxonRecord.newBuilder()
            .setCreated(
                2L) // This value for lastParsed and lastInterpreted since is greater that the Basic
            // record created date
            .setClassification(classification)
            .build();

    TemporalRecord temporalRecord =
        TemporalRecord.newBuilder()
            .setId("1")
            .setDateIdentified("2019-11-12T13:24:56.963591")
            .setModified("2019-04-15T17:17")
            .build();

    TaggedValueRecord taggedValueRecord =
        TaggedValueRecord.newBuilder()
            .setId("1")
            .setTaggedValues(
                Collections.singletonMap(
                    GbifInternalTerm.collectionKey.qualifiedName(),
                    "7ddf754f-d193-4cc9-b351-99906754a03b"))
            .build();

    // When
    OccurrenceHdfsRecord hdfsRecord =
        toOccurrenceHdfsRecord(
            basicRecord,
            metadataRecord,
            taxonRecord,
            temporalRecord,
            extendedRecord,
            taggedValueRecord);

    // Should
    // Test common fields
    Assert.assertEquals("1.0", hdfsRecord.getVerbatimdepth());
    Assert.assertEquals("C1", hdfsRecord.getCollectioncode());
    Assert.assertEquals("C1", hdfsRecord.getVCollectioncode());
    Assert.assertEquals("I1", hdfsRecord.getInstitutioncode());
    Assert.assertEquals("I1", hdfsRecord.getVInstitutioncode());
    Assert.assertEquals("CN1", hdfsRecord.getCatalognumber());
    Assert.assertEquals("CN1", hdfsRecord.getVCatalognumber());
    Assert.assertEquals("1", hdfsRecord.getIdentifier());
    Assert.assertEquals("1", hdfsRecord.getVIdentifier());
    Assert.assertEquals("quantity", hdfsRecord.getVOrganismquantity());
    Assert.assertEquals("type", hdfsRecord.getVOrganismquantitytype());
    Assert.assertEquals("unit", hdfsRecord.getVSamplesizeunit());
    Assert.assertEquals("value", hdfsRecord.getVSamplesizevalue());
    Assert.assertEquals("recordedBy", hdfsRecord.getVRecordedby());
    Assert.assertEquals("identifiedBy", hdfsRecord.getIdentifiedby());
    Assert.assertEquals("13123|21312", hdfsRecord.getVIdentifiedbyid());
    Assert.assertEquals("53453|5785", hdfsRecord.getVRecordedbyid());
    Assert.assertEquals(OccurrenceStatus.ABSENT.name(), hdfsRecord.getVOccurrencestatus());
    Assert.assertEquals("0", hdfsRecord.getVIndividualcount());

    // Test fields names with reserved words
    Assert.assertEquals("CLASS", hdfsRecord.getClass$());
    Assert.assertEquals("classs", hdfsRecord.getVClass());
    Assert.assertEquals("format", hdfsRecord.getFormat());
    Assert.assertEquals("format", hdfsRecord.getVFormat());
    Assert.assertEquals("ORDER", hdfsRecord.getOrder());
    Assert.assertEquals("order", hdfsRecord.getVOrder());
    Assert.assertEquals("group", hdfsRecord.getGroup());
    Assert.assertEquals("group", hdfsRecord.getVGroup());
    Assert.assertEquals("26/06/2019", hdfsRecord.getDate());
    Assert.assertEquals("26/06/2019", hdfsRecord.getVDate());

    // Test temporal fields
    Assert.assertNotNull(hdfsRecord.getDateidentified());
    Assert.assertNotNull(hdfsRecord.getModified());

    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), hdfsRecord.getBasisofrecord());
    Assert.assertEquals(
        BasisOfRecord.HUMAN_OBSERVATION.name().toLowerCase(), hdfsRecord.getVBasisofrecord());
    Assert.assertNull(hdfsRecord.getLifestage());
    Assert.assertEquals("adultss", hdfsRecord.getVLifestage());
    Assert.assertEquals(taxonRecord.getCreated(), hdfsRecord.getLastparsed());
    Assert.assertEquals(taxonRecord.getCreated(), hdfsRecord.getLastinterpreted());
    Assert.assertEquals("7ddf754f-d193-4cc9-b351-99906754a03b", hdfsRecord.getCollectionkey());
    Assert.assertEquals(License.CC0_1_0.name(), hdfsRecord.getLicense());
    Assert.assertEquals(Collections.singletonList("13123"), hdfsRecord.getRecordedbyid());
    Assert.assertEquals(Collections.singletonList("13123"), hdfsRecord.getIdentifiedbyid());
    Assert.assertEquals(OccurrenceStatus.ABSENT.name(), hdfsRecord.getOccurrencestatus());
    Assert.assertEquals(Integer.valueOf(0), hdfsRecord.getIndividualcount());
  }

  @Test
  public void multimediaMapperTest() {
    //
    MultimediaRecord multimediaRecord = new MultimediaRecord();
    multimediaRecord.setId("1");
    Multimedia multimedia = new Multimedia();
    multimedia.setType(MediaType.StillImage.name());
    multimedia.setLicense(License.CC_BY_4_0.name());
    multimedia.setSource("image.jpg");
    multimediaRecord.setMultimediaItems(Collections.singletonList(multimedia));
    OccurrenceHdfsRecord hdfsRecord = toOccurrenceHdfsRecord(multimediaRecord);

    // Testing de-serialization
    List<Multimedia> media = MediaSerDeserUtils.fromJson(hdfsRecord.getExtMultimedia());
    Assert.assertEquals(media.get(0), multimedia);
    Assert.assertTrue(hdfsRecord.getMediatype().contains(MediaType.StillImage.name()));
  }

  @Test
  public void basicRecordMapperTest() {
    // State
    long now = new Date().getTime();
    BasicRecord basicRecord = new BasicRecord();
    basicRecord.setBasisOfRecord(BasisOfRecord.HUMAN_OBSERVATION.name());
    basicRecord.setSex(Sex.HERMAPHRODITE.name());
    basicRecord.setIndividualCount(99);
    basicRecord.setLifeStage(LifeStage.GAMETE.name());
    basicRecord.setTypeStatus(TypeStatus.ALLOTYPE.name());
    basicRecord.setTypifiedName("noName");
    basicRecord.setEstablishmentMeans(EstablishmentMeans.INVASIVE.name());
    basicRecord.setCreated(now);
    basicRecord.setGbifId(1L);
    basicRecord.setOrganismQuantity(2d);
    basicRecord.setOrganismQuantityType("type");
    basicRecord.setSampleSizeUnit("unit");
    basicRecord.setSampleSizeValue(2d);
    basicRecord.setRelativeOrganismQuantity(2d);
    basicRecord.setLicense(License.UNSPECIFIED.name());

    // When
    OccurrenceHdfsRecord hdfsRecord = toOccurrenceHdfsRecord(basicRecord);

    // Should
    Assert.assertEquals(BasisOfRecord.HUMAN_OBSERVATION.name(), hdfsRecord.getBasisofrecord());
    Assert.assertEquals(Sex.HERMAPHRODITE.name(), hdfsRecord.getSex());
    Assert.assertEquals(Integer.valueOf(99), hdfsRecord.getIndividualcount());
    Assert.assertEquals(LifeStage.GAMETE.name(), hdfsRecord.getLifestage());
    Assert.assertEquals(TypeStatus.ALLOTYPE.name(), hdfsRecord.getTypestatus());
    Assert.assertEquals("noName", hdfsRecord.getTypifiedname());
    Assert.assertEquals(EstablishmentMeans.INVASIVE.name(), hdfsRecord.getEstablishmentmeans());
    Assert.assertEquals(Double.valueOf(2d), hdfsRecord.getOrganismquantity());
    Assert.assertEquals("type", hdfsRecord.getOrganismquantitytype());
    Assert.assertEquals("unit", hdfsRecord.getSamplesizeunit());
    Assert.assertEquals(Double.valueOf(2d), hdfsRecord.getSamplesizevalue());
    Assert.assertEquals(Double.valueOf(2d), hdfsRecord.getRelativeorganismquantity());
    Assert.assertNull(hdfsRecord.getLicense());
  }

  @Test
  public void taxonMapperTest() {
    // State
    List<RankedName> classification = new ArrayList<>();
    classification.add(
        RankedName.newBuilder().setKey(2).setRank(Rank.KINGDOM).setName("Archaea").build());
    classification.add(
        RankedName.newBuilder().setKey(79).setRank(Rank.PHYLUM).setName("Crenarchaeota").build());
    classification.add(
        RankedName.newBuilder()
            .setKey(8016360)
            .setRank(Rank.ORDER)
            .setName("Acidilobales")
            .build());
    classification.add(
        RankedName.newBuilder().setKey(292).setRank(Rank.CLASS).setName("Thermoprotei").build());
    classification.add(
        RankedName.newBuilder()
            .setKey(7785)
            .setRank(Rank.FAMILY)
            .setName("Caldisphaeraceae")
            .build());
    classification.add(
        RankedName.newBuilder()
            .setKey(1000002)
            .setRank(Rank.GENUS)
            .setName("Caldisphaera")
            .build());
    classification.add(
        RankedName.newBuilder()
            .setKey(1000003)
            .setRank(Rank.SPECIES)
            .setName("Caldisphaera lagunensis")
            .build());

    ParsedName parsedName =
        ParsedName.newBuilder()
            .setType(NameType.SCIENTIFIC)
            .setAbbreviated(Boolean.FALSE)
            .setBasionymAuthorship(
                Authorship.newBuilder()
                    .setYear("2003")
                    .setAuthors(Collections.singletonList("Itoh & al."))
                    .setExAuthors(Collections.emptyList())
                    .setEmpty(Boolean.FALSE)
                    .build())
            .setAutonym(Boolean.FALSE)
            .setBinomial(Boolean.TRUE)
            .setGenus("Caldisphaera")
            .setSpecificEpithet("lagunensis")
            .setNotho(NamePart.SPECIFIC)
            .setState(State.COMPLETE)
            .build();

    TaxonRecord taxonRecord = new TaxonRecord();
    RankedName rankedName =
        RankedName.newBuilder()
            .setKey(2492483)
            .setRank(Rank.SPECIES)
            .setName("Caldisphaera lagunensis Itoh & al., 2003")
            .build();

    taxonRecord.setUsage(rankedName);
    taxonRecord.setUsage(rankedName);
    taxonRecord.setAcceptedUsage(rankedName);
    taxonRecord.setSynonym(Boolean.FALSE);
    taxonRecord.setClassification(classification);
    taxonRecord.setUsageParsedName(parsedName);
    taxonRecord.setNomenclature(Nomenclature.newBuilder().setSource("nothing").build());

    // When
    OccurrenceHdfsRecord hdfsRecord = toOccurrenceHdfsRecord(taxonRecord);

    // Should
    Assert.assertEquals("Archaea", hdfsRecord.getKingdom());
    Assert.assertEquals(Integer.valueOf(2), hdfsRecord.getKingdomkey());

    Assert.assertEquals("Crenarchaeota", hdfsRecord.getPhylum());
    Assert.assertEquals(Integer.valueOf(79), hdfsRecord.getPhylumkey());

    Assert.assertEquals("Acidilobales", hdfsRecord.getOrder());
    Assert.assertEquals(Integer.valueOf(8016360), hdfsRecord.getOrderkey());

    Assert.assertEquals("Thermoprotei", hdfsRecord.getClass$());
    Assert.assertEquals(Integer.valueOf(292), hdfsRecord.getClasskey());

    Assert.assertEquals("Caldisphaeraceae", hdfsRecord.getFamily());
    Assert.assertEquals(Integer.valueOf(7785), hdfsRecord.getFamilykey());

    Assert.assertEquals("Caldisphaera", hdfsRecord.getGenus());
    Assert.assertEquals(Integer.valueOf(1000002), hdfsRecord.getGenuskey());

    Assert.assertEquals("Caldisphaera lagunensis", hdfsRecord.getSpecies());
    Assert.assertEquals(Integer.valueOf(1000003), hdfsRecord.getSpecieskey());

    Assert.assertEquals("2492483", hdfsRecord.getAcceptednameusageid());
    Assert.assertEquals(
        "Caldisphaera lagunensis Itoh & al., 2003", hdfsRecord.getAcceptedscientificname());
    Assert.assertEquals(Integer.valueOf(2492483), hdfsRecord.getAcceptedtaxonkey());

    Assert.assertEquals("Caldisphaera", hdfsRecord.getGenericname());
    Assert.assertEquals("lagunensis", hdfsRecord.getSpecificepithet());
  }

  @Test
  public void temporalMapperTest() {
    String rawEventDate = "2019-01-01";

    Long eventDate =
        LocalDate.of(2019, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();

    TemporalRecord temporalRecord =
        TemporalRecord.newBuilder()
            .setId("1")
            .setDay(1)
            .setYear(2019)
            .setMonth(1)
            .setStartDayOfYear(1)
            .setEventDate(EventDate.newBuilder().setLte(rawEventDate).build())
            .setDateIdentified(rawEventDate)
            .setModified(rawEventDate)
            .build();
    OccurrenceHdfsRecord hdfsRecord = toOccurrenceHdfsRecord(temporalRecord);
    Assert.assertEquals(Integer.valueOf(1), hdfsRecord.getDay());
    Assert.assertEquals(Integer.valueOf(1), hdfsRecord.getMonth());
    Assert.assertEquals(Integer.valueOf(2019), hdfsRecord.getYear());
    Assert.assertEquals("1", hdfsRecord.getStartdayofyear());
    Assert.assertEquals(eventDate, hdfsRecord.getEventdate());
    Assert.assertEquals(eventDate, hdfsRecord.getDateidentified());
    Assert.assertEquals(eventDate, hdfsRecord.getModified());
  }

  @Test
  public void metadataMapperTest() {
    // State
    String datasetKey = UUID.randomUUID().toString();
    String nodeKey = UUID.randomUUID().toString();
    String installationKey = UUID.randomUUID().toString();
    String organizationKey = UUID.randomUUID().toString();
    List<String> networkKey = Collections.singletonList(UUID.randomUUID().toString());

    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder()
            .setId("1")
            .setDatasetKey(datasetKey)
            .setCrawlId(1)
            .setDatasetPublishingCountry(Country.COSTA_RICA.getIso2LetterCode())
            .setLicense(License.CC_BY_4_0.name())
            .setNetworkKeys(networkKey)
            .setDatasetTitle("TestDataset")
            .setEndorsingNodeKey(nodeKey)
            .setInstallationKey(installationKey)
            .setLastCrawled(new Date().getTime())
            .setProtocol(EndpointType.DWC_ARCHIVE.name())
            .setPublisherTitle("Pub")
            .setPublishingOrganizationKey(organizationKey)
            .build();

    // When
    OccurrenceHdfsRecord hdfsRecord = toOccurrenceHdfsRecord(metadataRecord);

    // Should
    Assert.assertEquals(datasetKey, hdfsRecord.getDatasetkey());
    Assert.assertEquals(networkKey, hdfsRecord.getNetworkkey());
    Assert.assertEquals(installationKey, hdfsRecord.getInstallationkey());
    Assert.assertEquals(organizationKey, hdfsRecord.getPublishingorgkey());
    Assert.assertEquals(License.CC_BY_4_0.name(), hdfsRecord.getLicense());
  }

  @Test
  public void locationMapperTest() {
    // State
    LocationRecord locationRecord =
        LocationRecord.newBuilder()
            .setId("1")
            .setCountry(Country.COSTA_RICA.name())
            .setCountryCode(Country.COSTA_RICA.getIso2LetterCode())
            .setDecimalLatitude(9.934739)
            .setDecimalLongitude(-84.087502)
            .setContinent(Continent.NORTH_AMERICA.name())
            .setHasCoordinate(Boolean.TRUE)
            .setCoordinatePrecision(0.1)
            .setCoordinateUncertaintyInMeters(1.0)
            .setDepth(5.0)
            .setDepthAccuracy(0.1)
            .setElevation(0.0)
            .setElevationAccuracy(0.1)
            .setHasGeospatialIssue(Boolean.FALSE)
            .setRepatriated(Boolean.TRUE)
            .setStateProvince("Limon")
            .setWaterBody("Atlantic")
            .setMaximumDepthInMeters(0.1)
            .setMinimumDepthInMeters(0.1)
            .setMaximumDistanceAboveSurfaceInMeters(0.1)
            .setMaximumElevationInMeters(0.1)
            .setMinimumElevationInMeters(0.1)
            .build();

    // When
    OccurrenceHdfsRecord hdfsRecord = toOccurrenceHdfsRecord(locationRecord);

    // Should
    Assert.assertEquals(Country.COSTA_RICA.getIso2LetterCode(), hdfsRecord.getCountrycode());
    Assert.assertEquals(Double.valueOf(9.934739d), hdfsRecord.getDecimallatitude());
    Assert.assertEquals(Double.valueOf(-84.087502d), hdfsRecord.getDecimallongitude());
    Assert.assertEquals(Continent.NORTH_AMERICA.name(), hdfsRecord.getContinent());
    Assert.assertEquals(Boolean.TRUE, hdfsRecord.getHascoordinate());
    Assert.assertEquals(Double.valueOf(0.1d), hdfsRecord.getCoordinateprecision());
    Assert.assertEquals(Double.valueOf(1.0d), hdfsRecord.getCoordinateuncertaintyinmeters());
    Assert.assertEquals(Double.valueOf(5.0d), hdfsRecord.getDepth());
    Assert.assertEquals(Double.valueOf(0.1d), hdfsRecord.getDepthaccuracy());
    Assert.assertEquals(Double.valueOf(0.0d), hdfsRecord.getElevation());
    Assert.assertEquals(Double.valueOf(0.1d), hdfsRecord.getElevationaccuracy());
    Assert.assertEquals(Boolean.FALSE, hdfsRecord.getHasgeospatialissues());
    Assert.assertEquals(Boolean.TRUE, hdfsRecord.getRepatriated());
    Assert.assertEquals("Limon", hdfsRecord.getStateprovince());
    Assert.assertEquals("Atlantic", hdfsRecord.getWaterbody());
  }

  @Test
  public void issueMappingTest() {
    // State
    String[] issues = {
      OccurrenceIssue.IDENTIFIED_DATE_INVALID.name(),
      OccurrenceIssue.MODIFIED_DATE_INVALID.name(),
      OccurrenceIssue.RECORDED_DATE_UNLIKELY.name()
    };

    TemporalRecord temporalRecord =
        TemporalRecord.newBuilder()
            .setId("1")
            .setDay(1)
            .setYear(2019)
            .setMonth(1)
            .setStartDayOfYear(1)
            .setIssues(IssueRecord.newBuilder().setIssueList(Arrays.asList(issues)).build())
            .build();

    // When
    OccurrenceHdfsRecord hdfsRecord = toOccurrenceHdfsRecord(temporalRecord);

    // Should
    Assert.assertArrayEquals(issues, hdfsRecord.getIssue().toArray(new String[issues.length]));
  }

  @Test
  public void dateParserTest() {
    Date date = STRING_TO_DATE.apply("2019");
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-02");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(2, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-15T17:17:48.191 +02:00");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-15T17:17:48.191");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-15T17:17:48.023+02:00");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-11-12T13:24:56.963591");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(10, cal.get(Calendar.MONTH));
    assertEquals(12, cal.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  public void dateWithYearZeroTest() {
    Date date = STRING_TO_DATE.apply("0000");
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T00:00:01.100");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T17:17:48.191 +02:00");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T13:24:56.963591");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T17:17:48.023+02:00");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
  }
}
