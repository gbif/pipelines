package org.gbif.pipelines.core.converters;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.gbif.api.model.collections.lookup.Match.MatchType;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.ObisTerm;
import org.gbif.pipelines.core.utils.MediaSerDeser;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.event.EventHdfsRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.grscicoll.Match;
import org.junit.Assert;
import org.junit.Test;

public class EventHdfsRecordConverterTest {

  @Test
  public void extendedRecordAndCoreMapperTest() {

    // State
    final String multiValue1 = "multi 1";
    final String multiValue2 = "multi 2";

    Map<String, String> coreTerms = new HashMap<>();
    coreTerms.put(DwcTerm.verbatimDepth.simpleName(), "1.0");
    coreTerms.put(DwcTerm.collectionCode.simpleName(), "C1");
    coreTerms.put(DwcTerm.institutionCode.simpleName(), "I1");
    coreTerms.put(DcTerm.format.simpleName(), "format");
    coreTerms.put(DcTerm.date.simpleName(), "26/06/2019");
    coreTerms.put(DwcTerm.eventType.simpleName(), "event");
    coreTerms.put(DwcTerm.parentEventID.simpleName(), "parent");
    coreTerms.put(DwcTerm.sampleSizeUnit.simpleName(), "unit");
    coreTerms.put(DwcTerm.sampleSizeValue.simpleName(), "value");
    coreTerms.put(DwcTerm.eventDate.simpleName(), "2000/2010");
    coreTerms.put(DwcTerm.datasetID.simpleName(), multiValue1 + "|" + multiValue2);
    coreTerms.put(DwcTerm.datasetName.simpleName(), multiValue1 + "|" + multiValue2);
    coreTerms.put(DwcTerm.samplingProtocol.simpleName(), multiValue1 + "|" + multiValue2);
    coreTerms.put(GbifTerm.projectId.simpleName(), multiValue1 + "|" + multiValue2);
    coreTerms.put(DwcTerm.taxonConceptID.simpleName(), "v_taxonConceptID");
    coreTerms.put(DwcTerm.associatedSequences.simpleName(), "v_ad");
    coreTerms.put(DwcTerm.previousIdentifications.simpleName(), "v_previousIdentifications");
    coreTerms.put(DwcTerm.fundingAttributionID.simpleName(), multiValue1 + "|" + multiValue2);
    coreTerms.put(DwcTerm.fundingAttribution.simpleName(), multiValue1 + "|" + multiValue2);
    coreTerms.put(DwcTerm.projectTitle.simpleName(), multiValue1 + "|" + multiValue2);

    Map<String, List<Map<String, String>>> extensions = new HashMap<>();
    extensions.put(
        "http://rs.tdwg.org/ac/terms/Multimedia",
        Collections.singletonList(Collections.singletonMap("key", "value")));
    extensions.put(
        "http://data.ggbn.org/schemas/ggbn/terms/Amplification",
        Collections.singletonList(Collections.singletonMap("key", "value")));
    extensions.put(
        Extension.MEASUREMENT_OR_FACT.getRowType(),
        Arrays.asList(
            Map.of(DwcTerm.measurementType.qualifiedName(), "mt1"),
            Map.of(DwcTerm.measurementType.qualifiedName(), "mt2")));
    extensions.put(
        Extension.EXTENDED_MEASUREMENT_OR_FACT.getRowType(),
        Arrays.asList(
            Map.of(DwcTerm.measurementType.qualifiedName(), "mt3"),
            Map.of(ObisTerm.measurementTypeID.qualifiedName(), "mtid1")));

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(coreTerms)
            .setExtensions(extensions)
            .build();

    MetadataRecord metadataRecord =
        MetadataRecord.newBuilder()
            .setId("1")
            .setLicense(License.CC_BY_4_0.name())
            .setHostingOrganizationKey("hostOrgKey")
            .setProjectId(multiValue1)
            .build();

    EventCoreRecord eventCoreRecord =
        EventCoreRecord.newBuilder()
            .setId("1")
            .setCreated(1L)
            .setEventType(
                VocabularyConcept.newBuilder()
                    .setConcept("Event")
                    .setLineage(List.of("Event"))
                    .build())
            .setParentEventID("Parent")
            .setFundingAttributionID(Arrays.asList(multiValue1, multiValue2))
            .setProjectTitle(Arrays.asList(multiValue1, multiValue2))
            .setProjectID(Arrays.asList(multiValue1, multiValue2))
            .setSampleSizeUnit("unit")
            .setSampleSizeValue(1d)
            .setLicense(License.CC0_1_0.name())
            .setDatasetID(Arrays.asList(multiValue1, multiValue2))
            .setDatasetName(Arrays.asList(multiValue1, multiValue2))
            .setSamplingProtocol(Arrays.asList(multiValue1, multiValue2))
            .build();

    TemporalRecord temporalRecord =
        TemporalRecord.newBuilder()
            .setId("1")
            .setDateIdentified("2019-11-12T13:24:56.963591")
            .setModified("2019-04-15T17:17")
            .setEventDate(
                EventDate.newBuilder()
                    .setGte("2000")
                    .setLte("2010")
                    .setInterval("2000/2010")
                    .build())
            .build();

    IdentifierRecord identifierRecord =
        IdentifierRecord.newBuilder().setId("1").setInternalId("777").build();

    Humboldt humboldt =
        Humboldt.newBuilder()
            .setTargetTaxonomicScope(
                List.of(
                    TaxonHumboldtRecord.newBuilder()
                        .setClassification(
                            List.of(
                                RankedName.newBuilder().setRank("rank").setName("name").build()))
                        .build()))
            .setTargetLifeStageScope(
                List.of(
                    VocabularyConcept.newBuilder()
                        .setConcept("c1")
                        .setLineage(List.of("c0", "c1"))
                        .build(),
                    VocabularyConcept.newBuilder()
                        .setConcept("c11")
                        .setLineage(List.of("c00", "c11"))
                        .build()))
            .build();
    HumboldtRecord humboldtRecord =
        HumboldtRecord.newBuilder()
            .setId("1")
            .setCreated(13214L)
            .setHumboldtItems(List.of(humboldt))
            .build();

    // When
    EventHdfsRecord hdfsRecord =
        EventHdfsRecordConverter.builder()
            .eventCoreRecord(eventCoreRecord)
            .metadataRecord(metadataRecord)
            .identifierRecord(identifierRecord)
            .temporalRecord(temporalRecord)
            .extendedRecord(extendedRecord)
            .humboldtRecord(humboldtRecord)
            .build()
            .convert();

    // Should
    // Test common fields
    Assert.assertEquals("C1", hdfsRecord.getCollectioncode());
    Assert.assertEquals("C1", hdfsRecord.getVCollectioncode());
    Assert.assertEquals("I1", hdfsRecord.getInstitutioncode());
    Assert.assertEquals("I1", hdfsRecord.getVInstitutioncode());
    Assert.assertEquals("unit", hdfsRecord.getVSamplesizeunit());
    Assert.assertEquals("value", hdfsRecord.getVSamplesizevalue());
    Assert.assertEquals("2000/2010", hdfsRecord.getVEventdate());
    Assert.assertEquals(multiValue1 + "|" + multiValue2, hdfsRecord.getVDatasetid());
    Assert.assertEquals(Arrays.asList(multiValue1, multiValue2), hdfsRecord.getDatasetid());
    Assert.assertEquals(multiValue1 + "|" + multiValue2, hdfsRecord.getVDatasetname());
    Assert.assertEquals(Arrays.asList(multiValue1, multiValue2), hdfsRecord.getDatasetname());
    Assert.assertEquals(multiValue1 + "|" + multiValue2, hdfsRecord.getVSamplingprotocol());
    Assert.assertEquals(Arrays.asList(multiValue1, multiValue2), hdfsRecord.getSamplingprotocol());
    Assert.assertEquals(Arrays.asList(multiValue1, multiValue2), hdfsRecord.getProjectid());
    Assert.assertEquals(
        Arrays.asList(multiValue1, multiValue2), hdfsRecord.getFundingattributionid());
    Assert.assertEquals(multiValue1 + "|" + multiValue2, hdfsRecord.getVFundingattributionid());
    Assert.assertEquals(Arrays.asList(multiValue1, multiValue2), hdfsRecord.getProjecttitle());
    Assert.assertEquals(multiValue1 + "|" + multiValue2, hdfsRecord.getVProjecttitle());
    Assert.assertEquals("Event", hdfsRecord.getEventtype().getConcept());
    Assert.assertEquals("event", hdfsRecord.getVEventtype());
    Assert.assertEquals("parent", hdfsRecord.getVParenteventid());
    Assert.assertEquals("Parent", hdfsRecord.getParenteventid());

    // Test temporal fields
    Assert.assertNotNull(hdfsRecord.getDateidentified());
    Assert.assertNotNull(hdfsRecord.getModified());
    Assert.assertNotNull(hdfsRecord.getEventdate());

    Assert.assertEquals(License.CC0_1_0.name(), hdfsRecord.getLicense());
    Assert.assertEquals("2000/2010", hdfsRecord.getEventdate());
    Assert.assertEquals(
        LocalDate.of(2000, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC).getEpochSecond(),
        hdfsRecord.getEventdategte().longValue());
    Assert.assertEquals(
        LocalDateTime.of(2010, 12, 31, 23, 59, 59).toInstant(ZoneOffset.UTC).getEpochSecond(),
        hdfsRecord.getEventdatelte().longValue());
    Assert.assertEquals(
        metadataRecord.getHostingOrganizationKey(), hdfsRecord.getHostingorganizationkey());

    Assert.assertEquals("v_previousIdentifications", hdfsRecord.getVPreviousidentifications());

    // extensions
    Assert.assertEquals(4, hdfsRecord.getDwcaextension().size());
    Assert.assertTrue(
        hdfsRecord.getDwcaextension().contains("http://rs.tdwg.org/ac/terms/Multimedia"));
    Assert.assertTrue(
        hdfsRecord
            .getDwcaextension()
            .contains("http://data.ggbn.org/schemas/ggbn/terms/Amplification"));

    // MoF
    Assert.assertEquals(3, hdfsRecord.getMeasurementtype().size());
    Assert.assertEquals(1, hdfsRecord.getMeasurementtypeid().size());

    // Humboldt
    Assert.assertTrue(
        hdfsRecord
            .getExtHumboldt()
            .contains(
                "\"targetLifeStageScope\" : {\n"
                    + "    \"concepts\" : [ \"c1\", \"c11\" ],\n"
                    + "    \"lineage\" : [ \"c0\", \"c1\", \"c00\", \"c11\" ]\n"
                    + "  }"));
  }

  @Test
  public void multimediaMapperTest() {
    // State
    String[] issues = {OccurrenceIssue.MULTIMEDIA_DATE_INVALID.name()};

    MultimediaRecord multimediaRecord = new MultimediaRecord();
    multimediaRecord.setId("1");
    Multimedia multimedia = new Multimedia();
    multimedia.setType(MediaType.StillImage.name());
    multimedia.setLicense(License.CC_BY_4_0.name());
    multimedia.setSource("image.jpg");
    multimediaRecord.setMultimediaItems(Collections.singletonList(multimedia));
    multimediaRecord.setIssues(
        IssueRecord.newBuilder().setIssueList(Arrays.asList(issues)).build());

    // When
    OccurrenceHdfsRecord hdfsRecord =
        OccurrenceHdfsRecordConverter.builder()
            .multimediaRecord(multimediaRecord)
            .build()
            .convert();

    // Should
    // Testing de-serialization
    List<Multimedia> media = MediaSerDeser.multimediaFromJson(hdfsRecord.getExtMultimedia());
    Assert.assertEquals(media.get(0), multimedia);
    Assert.assertTrue(hdfsRecord.getMediatype().contains(MediaType.StillImage.name()));
    Assert.assertTrue(
        hdfsRecord.getIssue().contains(OccurrenceIssue.MULTIMEDIA_DATE_INVALID.name()));
  }

  @Test
  public void gbifIdRecordMapperTest() {
    // State
    long now = new Date().getTime();
    IdentifierRecord identifierRecord = new IdentifierRecord();
    identifierRecord.setFirstLoaded(now);
    identifierRecord.setInternalId("1");

    // When
    OccurrenceHdfsRecord hdfsRecord =
        OccurrenceHdfsRecordConverter.builder()
            .identifierRecord(identifierRecord)
            .build()
            .convert();

    // Should
    Assert.assertEquals("1", hdfsRecord.getGbifid());
  }

  @Test
  public void temporalMapperTest() {
    String rawEventDate = "2019-01";

    Long eventDate =
        LocalDate.of(2019, 1, 1).atStartOfDay().toInstant(ZoneOffset.UTC).getEpochSecond();

    TemporalRecord temporalRecord =
        TemporalRecord.newBuilder()
            .setId("1")
            .setDay(1)
            .setYear(2019)
            .setMonth(1)
            .setStartDayOfYear(1)
            .setEndDayOfYear(1)
            .setEventDate(
                EventDate.newBuilder()
                    .setGte(rawEventDate)
                    .setLte(rawEventDate)
                    .setInterval("2019-01")
                    .build())
            .setDateIdentified(rawEventDate)
            .setModified(rawEventDate)
            .build();

    EventHdfsRecord hdfsRecord =
        EventHdfsRecordConverter.builder().temporalRecord(temporalRecord).build().convert();

    Assert.assertEquals(Integer.valueOf(1), hdfsRecord.getDay());
    Assert.assertEquals(Integer.valueOf(1), hdfsRecord.getMonth());
    Assert.assertEquals(Integer.valueOf(2019), hdfsRecord.getYear());
    Assert.assertEquals(Integer.valueOf(1), hdfsRecord.getStartdayofyear());
    Assert.assertEquals(Integer.valueOf(1), hdfsRecord.getEnddayofyear());
    Assert.assertEquals("2019-01", hdfsRecord.getEventdate());
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
    EventHdfsRecord hdfsRecord =
        EventHdfsRecordConverter.builder().metadataRecord(metadataRecord).build().convert();

    // Should
    Assert.assertEquals(datasetKey, hdfsRecord.getDatasetkey());
    Assert.assertEquals(networkKey, hdfsRecord.getNetworkkey());
    Assert.assertEquals(installationKey, hdfsRecord.getInstallationkey());
    Assert.assertEquals(organizationKey, hdfsRecord.getPublishingorgkey());
    Assert.assertEquals(License.CC_BY_4_0.name(), hdfsRecord.getLicense());
    Assert.assertEquals(metadataRecord.getDatasetTitle(), hdfsRecord.getDatasettitle());
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
            .setDistanceFromCentroidInMeters(10.0)
            .build();

    // When
    EventHdfsRecord hdfsRecord =
        EventHdfsRecordConverter.builder().locationRecord(locationRecord).build().convert();

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
    Assert.assertEquals(Double.valueOf(10.0d), hdfsRecord.getDistancefromcentroidinmeters());
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
    OccurrenceHdfsRecord hdfsRecord =
        OccurrenceHdfsRecordConverter.builder().temporalRecord(temporalRecord).build().convert();

    // Should
    Assert.assertArrayEquals(issues, hdfsRecord.getIssue().toArray(new String[issues.length]));
  }

  @Test
  public void grscicollMapperTest() {
    // State
    String[] issues = {OccurrenceIssue.INSTITUTION_COLLECTION_MISMATCH.name()};

    Match institutionMatch =
        Match.newBuilder()
            .setKey("cb0098db-6ff6-4a5d-ad29-51348d114e41")
            .setMatchType(MatchType.EXACT.name())
            .build();

    Match collectionMatch =
        Match.newBuilder()
            .setKey("5c692584-d517-48e8-93a8-a916ba131d9b")
            .setMatchType(MatchType.FUZZY.name())
            .build();

    GrscicollRecord grscicollRecord =
        GrscicollRecord.newBuilder()
            .setId("1")
            .setInstitutionMatch(institutionMatch)
            .setCollectionMatch(collectionMatch)
            .setIssues(IssueRecord.newBuilder().setIssueList(Arrays.asList(issues)).build())
            .build();

    // When
    OccurrenceHdfsRecord hdfsRecord =
        OccurrenceHdfsRecordConverter.builder().grscicollRecord(grscicollRecord).build().convert();

    // Should
    Assert.assertEquals(institutionMatch.getKey(), hdfsRecord.getInstitutionkey());
    Assert.assertEquals(collectionMatch.getKey(), hdfsRecord.getCollectionkey());
    Assert.assertTrue(
        hdfsRecord.getIssue().contains(OccurrenceIssue.INSTITUTION_COLLECTION_MISMATCH.name()));
  }
}
