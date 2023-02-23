package org.gbif.pipelines.core.converters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.model.collections.lookup.Match.MatchType;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Indexing;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.Authorship;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MachineTag;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.NamePart;
import org.gbif.pipelines.io.avro.NameRank;
import org.gbif.pipelines.io.avro.NameType;
import org.gbif.pipelines.io.avro.NomCode;
import org.gbif.pipelines.io.avro.ParsedName;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.State;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.grscicoll.Match;
import org.junit.Test;

public class OccurrenceJsonConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void converterTest() throws Exception {
    // State
    final String multivalue1 = "mv;à1";
    final String expectedMultivalue1 = "mv;à1";
    final String multivalue2 = "mv2";

    Map<String, String> erMap = new HashMap<>(5);
    erMap.put("http://rs.tdwg.org/dwc/terms/locality", "something:{something}");
    erMap.put("http://purl.org/dc/terms/remark", "{\"something\":1}{\"something\":1}");
    erMap.put(DwcTerm.recordedBy.qualifiedName(), multivalue1 + "|" + multivalue2);
    erMap.put(DwcTerm.footprintWKT.qualifiedName(), "footprintWKTfootprintWKTfootprintWKT");
    erMap.put(DwcTerm.catalogNumber.qualifiedName(), "catalogNumber");
    erMap.put(DwcTerm.collectionCode.qualifiedName(), "collectionCode");
    erMap.put(DwcTerm.eventID.qualifiedName(), "eventId");
    erMap.put(DwcTerm.recordNumber.qualifiedName(), "recordNumber");
    erMap.put(DwcTerm.occurrenceID.qualifiedName(), "occurrenceID");
    erMap.put(DwcTerm.organismID.qualifiedName(), "organismID");
    erMap.put(DwcTerm.parentEventID.qualifiedName(), "parentEventId");
    erMap.put(DwcTerm.institutionCode.qualifiedName(), "institutionCode");
    erMap.put(DwcTerm.scientificName.qualifiedName(), "scientificName");
    erMap.put(DwcTerm.taxonID.qualifiedName(), "taxonID");
    erMap.put(DwcTerm.scientificName.qualifiedName(), "scientificName");
    erMap.put(GbifTerm.projectId.qualifiedName(), multivalue1 + "|" + multivalue2);

    MetadataRecord mr =
        MetadataRecord.newBuilder()
            .setId("777")
            .setCrawlId(1)
            .setLastCrawled(1647941576L)
            .setDatasetKey("datatesKey")
            .setLicense(License.CC0_1_0.name())
            .setHostingOrganizationKey("hostOrgKey")
            .setDatasetPublishingCountry("setDatasetPublishingCountry")
            .setDatasetTitle("setDatasetTitle")
            .setEndorsingNodeKey("setEndorsingNodeKey")
            .setProgrammeAcronym("setProgrammeAcronym")
            .setProjectId(multivalue2)
            .setProtocol("setProtocol")
            .setPublisherTitle("setPublisherTitle")
            .setPublishingOrganizationKey("setPublishingOrganizationKey")
            .setInstallationKey("setInstallationKey")
            .setNetworkKeys(Collections.singletonList("setNetworkKeys"))
            .setMachineTags(
                Collections.singletonList(
                    MachineTag.newBuilder()
                        .setName("Name")
                        .setNamespace("Namespace")
                        .setValue("Value")
                        .build()))
            .build();

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreRowType("core")
            .setCoreTerms(erMap)
            .setExtensions(
                Collections.singletonMap(
                    "http://rs.tdwg.org/ac/terms/Multimedia",
                    Collections.singletonList(Collections.singletonMap("k", "v"))))
            .build();

    IdentifierRecord id = IdentifierRecord.newBuilder().setId("777").setInternalId("111").build();

    ClusteringRecord cr = ClusteringRecord.newBuilder().setId("777").setIsClustered(true).build();

    BasicRecord br =
        BasicRecord.newBuilder()
            .setId("777")
            .setBasisOfRecord("setBasisOfRecord")
            .setOrganismQuantity(2d)
            .setOrganismQuantityType("OrganismQuantityType")
            .setSampleSizeUnit("SampleSizeUnit")
            .setSampleSizeValue(2d)
            .setRelativeOrganismQuantity(0.001d)
            .setLicense(License.CC_BY_NC_4_0.name())
            .setOccurrenceStatus(OccurrenceStatus.PRESENT.name())
            .setSex("sex")
            .setReferences("setReferences")
            .setTypifiedName("setTypifiedName")
            .setIndividualCount(10)
            .setLifeStage(
                VocabularyConcept.newBuilder()
                    .setConcept("bla1")
                    .setLineage(Collections.singletonList("bla1_1"))
                    .build())
            .setPathway(
                VocabularyConcept.newBuilder()
                    .setConcept("bla2")
                    .setLineage(Collections.singletonList("bla2_1"))
                    .build())
            .setEstablishmentMeans(
                VocabularyConcept.newBuilder()
                    .setConcept("bla3")
                    .setLineage(Collections.singletonList("bla3_1"))
                    .build())
            .setDegreeOfEstablishment(
                VocabularyConcept.newBuilder()
                    .setConcept("bla4")
                    .setLineage(Collections.singletonList("bla4_1"))
                    .build())
            .setRecordedByIds(
                Collections.singletonList(
                    AgentIdentifier.newBuilder()
                        .setType(AgentIdentifierType.OTHER.name())
                        .setValue("someId")
                        .build()))
            .setIdentifiedByIds(
                Collections.singletonList(
                    AgentIdentifier.newBuilder()
                        .setType(AgentIdentifierType.OTHER.name())
                        .setValue("someId")
                        .build()))
            .setDatasetID(Arrays.asList(multivalue1, multivalue2))
            .setDatasetName(Arrays.asList(multivalue1, multivalue2))
            .setOtherCatalogNumbers(Arrays.asList(multivalue1, multivalue2))
            .setRecordedBy(Arrays.asList(multivalue1, multivalue2))
            .setIdentifiedBy(Arrays.asList(multivalue1, multivalue2))
            .setPreparations(Arrays.asList(multivalue1, "\u001E" + multivalue2))
            .setSamplingProtocol(Arrays.asList(multivalue1, multivalue2))
            .setTypeStatus(Arrays.asList(TypeStatus.TYPE.name(), TypeStatus.TYPE_SPECIES.name()))
            .setProjectId(Arrays.asList(multivalue1, multivalue2))
            .build();

    TemporalRecord tmr =
        TemporalRecord.newBuilder()
            .setId("777")
            .setCreated(0L)
            .setEventDate(EventDate.newBuilder().setGte("2011-01").setLte("2018-01").build())
            .setDay(1)
            .setMonth(1)
            .setYear(2011)
            .setStartDayOfYear(1)
            .setEndDayOfYear(365)
            .setModified("11-11-2021")
            .setDateIdentified("10-01-2020")
            .build();
    tmr.getIssues().getIssueList().add(OccurrenceIssue.ZERO_COORDINATE.name());

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId("777")
            .setCreated(1555454275758L)
            .setCountry("Country")
            .setCountryCode("Code 1'2\"")
            .setDecimalLatitude(1d)
            .setDecimalLongitude(2d)
            .setContinent("something{something}")
            .setLocality("[68]")
            .setCoordinatePrecision(2d)
            .setCoordinateUncertaintyInMeters(3d)
            .setDepth(4d)
            .setDepthAccuracy(4d)
            .setElevation(5d)
            .setPublishingCountry("setPublishingCountry")
            .setElevationAccuracy(5d)
            .setFootprintWKT("setFootprintWKT")
            .setHasCoordinate(true)
            .setHasGeospatialIssue(false)
            .setMaximumDepthInMeters(7d)
            .setMaximumElevationInMeters(8d)
            .setMaximumDistanceAboveSurfaceInMeters(9d)
            .setMinimumDepthInMeters(7d)
            .setMinimumElevationInMeters(8d)
            .setMinimumDistanceAboveSurfaceInMeters(9d)
            .setWaterBody("setWaterBody")
            .setStateProvince("setStateProvince")
            .setRepatriated(true)
            .setGadm(
                GadmFeatures.newBuilder()
                    .setLevel0Gid("XAA_1")
                    .setLevel0Name("Countryland")
                    .setLevel1Gid("XAA.1_1")
                    .setLevel1Name("Countyshire")
                    .setLevel2Gid("XAA.1.2_1")
                    .setLevel2Name("Muni Cipality")
                    .setLevel3Gid("XAA.1.3_1")
                    .setLevel3Name("Level 3 Cipality")
                    .build())
            .setDistanceFromCentroidInMeters(10d)
            .build();
    lr.getIssues().getIssueList().add(OccurrenceIssue.BASIS_OF_RECORD_INVALID.name());

    List<RankedName> rankedNameList = new ArrayList<>();

    RankedName synonym =
        RankedName.newBuilder().setKey(10).setName("synonym").setRank(Rank.SPECIES).build();
    RankedName au =
        RankedName.newBuilder().setKey(11).setName("accepted usage").setRank(Rank.SPECIES).build();

    RankedName name1 =
        RankedName.newBuilder().setKey(1).setName("KINGDOM").setRank(Rank.KINGDOM).build();
    RankedName name2 =
        RankedName.newBuilder().setKey(2).setName("PHYLUM").setRank(Rank.PHYLUM).build();
    RankedName name3 =
        RankedName.newBuilder().setKey(3).setName("CLASS").setRank(Rank.CLASS).build();
    RankedName name4 =
        RankedName.newBuilder().setKey(4).setName("ORDER").setRank(Rank.ORDER).build();
    RankedName name5 =
        RankedName.newBuilder().setKey(5).setName("FAMILY").setRank(Rank.FAMILY).build();
    RankedName name6 =
        RankedName.newBuilder().setKey(6).setName("GENUS").setRank(Rank.GENUS).build();
    RankedName name7 =
        RankedName.newBuilder().setKey(7).setName("SPECIES").setRank(Rank.SPECIES).build();

    rankedNameList.add(name1);
    rankedNameList.add(name2);
    rankedNameList.add(name3);
    rankedNameList.add(name4);
    rankedNameList.add(name5);
    rankedNameList.add(name6);
    rankedNameList.add(name7);

    Diagnostic diagnostic =
        Diagnostic.newBuilder()
            .setStatus(Status.ACCEPTED)
            .setConfidence(1)
            .setMatchType(org.gbif.pipelines.io.avro.MatchType.EXACT)
            .setNote("note")
            .setLineage(Collections.singletonList("setLineage"))
            .setAlternatives(
                Collections.singletonList(
                    TaxonRecord.newBuilder()
                        .setId("888")
                        .setAcceptedUsage(au)
                        .setClassification(rankedNameList)
                        .setUsage(synonym)
                        .setSynonym(true)
                        .setIucnRedListCategoryCode("setIucnRedListCategoryCode")
                        .build()))
            .build();

    TaxonRecord tr =
        TaxonRecord.newBuilder()
            .setId("777")
            .setAcceptedUsage(au)
            .setClassification(rankedNameList)
            .setUsage(synonym)
            .setSynonym(true)
            .setIucnRedListCategoryCode("setIucnRedListCategoryCode")
            .setUsageParsedName(
                ParsedName.newBuilder()
                    .setGenus("setGenus")
                    .setUninomial("setUninomial")
                    .setAbbreviated(false)
                    .setAutonym(false)
                    .setBinomial(false)
                    .setCandidatus(false)
                    .setCode(NomCode.BACTERIAL)
                    .setDoubtful(false)
                    .setIncomplete(false)
                    .setIndetermined(false)
                    .setInfraspecificEpithet("infraspecificEpithet")
                    .setRank(NameRank.ABERRATION)
                    .setNotho(NamePart.GENERIC)
                    .setSpecificEpithet("specificEpithet")
                    .setState(State.COMPLETE)
                    .setTerminalEpithet("terminalEpithet")
                    .setTrinomial(false)
                    .setType(NameType.HYBRID_FORMULA)
                    .setBasionymAuthorship(
                        Authorship.newBuilder()
                            .setYear("2000")
                            .setAuthors(Collections.singletonList("setBasionymAuthorship"))
                            .setExAuthors(Collections.singletonList("setBasionymAuthorship"))
                            .setEmpty(true)
                            .build())
                    .setCombinationAuthorship(
                        Authorship.newBuilder()
                            .setYear("2020")
                            .setAuthors(Collections.singletonList("setCombinationAuthorship"))
                            .setExAuthors(Collections.singletonList("setCombinationAuthorship"))
                            .setEmpty(false)
                            .build())
                    .build())
            .setDiagnostics(diagnostic)
            .build();

    // grscicoll
    Match institutionMatch =
        Match.newBuilder()
            .setKey("cb0098db-6ff6-4a5d-ad29-51348d114e41")
            .setMatchType(MatchType.FUZZY.name())
            .build();

    Match collectionMatch =
        Match.newBuilder()
            .setKey("23123123123123122312313123123122312231")
            .setMatchType(MatchType.EXACT.name())
            .build();

    GrscicollRecord gr =
        GrscicollRecord.newBuilder()
            .setId("1")
            .setInstitutionMatch(institutionMatch)
            .setCollectionMatch(collectionMatch)
            .build();
    gr.getIssues().getIssueList().add(OccurrenceIssue.INSTITUTION_MATCH_FUZZY.name());

    // State
    Multimedia stillImage = new Multimedia();
    stillImage.setType(MediaType.StillImage.name());
    stillImage.setFormat("image/jpeg");
    stillImage.setLicense("somelicense");
    stillImage.setIdentifier("identifier");
    stillImage.setAudience("audience");
    stillImage.setContributor("contributor");
    stillImage.setCreated("created");
    stillImage.setCreator("creator");
    stillImage.setDescription("description");
    stillImage.setPublisher("publisher");
    stillImage.setReferences("references");
    stillImage.setRightsHolder("rightsHolder");
    stillImage.setSource("source");
    stillImage.setTitle("title");
    stillImage.setDatasetId("datasetId");

    Multimedia movingImage = new Multimedia();
    movingImage.setType(MediaType.MovingImage.name());
    movingImage.setFormat("video/mp4");
    movingImage.setLicense("somelicense");
    movingImage.setIdentifier("identifier");
    movingImage.setAudience("audience");
    movingImage.setContributor("contributor");
    movingImage.setCreated("created");
    movingImage.setCreator("creator");
    movingImage.setDescription("description");
    movingImage.setPublisher("publisher");
    movingImage.setReferences("references");
    movingImage.setRightsHolder("rightsHolder");
    movingImage.setSource("source");
    movingImage.setTitle("title");
    movingImage.setDatasetId("datasetId");

    MultimediaRecord mmr =
        MultimediaRecord.newBuilder()
            .setId("777")
            .setMultimediaItems(Arrays.asList(stillImage, movingImage))
            .build();

    // When
    String json =
        OccurrenceJsonConverter.builder()
            .basic(br)
            .identifier(id)
            .clustering(cr)
            .metadata(mr)
            .verbatim(er)
            .temporal(tmr)
            .location(lr)
            .taxon(tr)
            .grscicoll(gr)
            .multimedia(mmr)
            .build()
            .toJsonWithNulls();

    JsonNode result = MAPPER.readTree(json);

    // Should
    assertTrue(JsonValidationUtils.isValid(result.toString()));

    assertEquals(mr.getDatasetKey(), result.path(Indexing.DATASET_KEY).asText());
    assertEquals(mr.getCrawlId(), (Integer) result.path(Indexing.CRAWL_ID).asInt());
    assertEquals("CC_BY_NC_4_0", result.path(Indexing.LICENSE).asText());
    assertEquals(
        mr.getHostingOrganizationKey(), result.path(Indexing.HOSTING_ORGANIZATION_KEY).asText());
    assertEquals(mr.getId(), result.path(Indexing.ID).asText());
    assertEquals("2011-01-01T00:00", result.path(Indexing.EVENT_DATE_SINGLE).asText());
    assertEquals("2011", result.path(Indexing.YEAR).asText());
    assertEquals("1", result.path(Indexing.MONTH).asText());
    assertEquals("1", result.path(Indexing.DAY).asText());
    assertEquals(
        "{\"gte\":\"2011-01\",\"lte\":\"2018-01\"}", result.path(Indexing.EVENT_DATE).toString());
    assertEquals("1", result.path(Indexing.START_DAY_OF_YEAR).asText());
    assertEquals("{\"lon\":2.0,\"lat\":1.0}", result.path(Indexing.COORDINATES).toString());
    assertEquals("1.0", result.path(Indexing.DECIMAL_LATITUDE).asText());
    assertEquals("2.0", result.path(Indexing.DECIMAL_LONGITUDE).asText());
    assertEquals("POINT (2.0 1.0)", result.path(Indexing.SCOORDINATES).asText());
    assertEquals("Country", result.path(Indexing.COUNTRY).asText());
    assertEquals("Code 1'2\"", result.path(Indexing.COUNTRY_CODE).asText());
    assertEquals("[68]", result.path(Indexing.LOCALITY).asText());
    assertTrue(result.path(Indexing.IS_CLUSTERED).asBoolean());
    assertEquals(
        "[\"" + expectedMultivalue1 + "\",\"" + multivalue2 + "\"]",
        result.path(Indexing.DATASET_ID).toString());
    assertEquals(
        "[\"" + expectedMultivalue1 + "\",\"" + multivalue2 + "\"]",
        result.path(Indexing.DATASET_NAME).toString());
    assertEquals(
        "[\"" + expectedMultivalue1 + "\",\"" + multivalue2 + "\"]",
        result.path(Indexing.OTHER_CATALOG_NUMBERS).toString());
    assertEquals(
        "\"" + expectedMultivalue1 + "|" + multivalue2 + "\"",
        result.path(Indexing.OTHER_CATALOG_NUMBERS_JOINED).toString());
    assertEquals(
        "[\"" + expectedMultivalue1 + "\",\"" + multivalue2 + "\"]",
        result.path(Indexing.RECORDED_BY).toString());
    assertEquals(
        "\"" + expectedMultivalue1 + "|" + multivalue2 + "\"",
        result.path(Indexing.RECORDED_BY_JOINED).toString());
    assertEquals(
        "[\"" + expectedMultivalue1 + "\",\"" + multivalue2 + "\"]",
        result.path(Indexing.IDENTIFIED_BY).toString());
    assertEquals(
        "\"" + expectedMultivalue1 + "|" + multivalue2 + "\"",
        result.path(Indexing.IDENTIFIED_BY_JOINED).toString());
    assertEquals(
        "[\"" + expectedMultivalue1 + "\",\"" + multivalue2 + "\"]",
        result.path(Indexing.PREPARATIONS).toString());
    assertEquals(
        "\"" + expectedMultivalue1 + "|," + multivalue2 + "\"",
        result.path(Indexing.PREPARATIONS_JOINED).toString());
    assertEquals(
        "[\"" + expectedMultivalue1 + "\",\"" + multivalue2 + "\"]",
        result.path(Indexing.SAMPLING_PROTOCOL).toString());
    assertEquals(
        "\"" + expectedMultivalue1 + "|" + multivalue2 + "\"",
        result.path(Indexing.SAMPLING_PROTOCOL_JOINED).toString());
    assertEquals(
        "[\"" + TypeStatus.TYPE.name() + "\",\"" + TypeStatus.TYPE_SPECIES.name() + "\"]",
        result.path(Indexing.TYPE_STATUS).toString());

    ArrayNode projectIdArray = (ArrayNode) result.path(Indexing.PROJECT_ID);
    assertEquals(2, projectIdArray.size());
    projectIdArray
        .elements()
        .forEachRemaining(
            n -> assertTrue(n.asText().equals(multivalue1) || n.asText().equals(multivalue2)));
    List<String> projectIdJoined =
        Arrays.asList(result.path(Indexing.PROJECT_ID_JOINED).asText().split("\\|"));
    assertEquals(2, projectIdJoined.size());
    assertTrue(projectIdJoined.contains(multivalue1));
    assertTrue(projectIdJoined.contains(multivalue2));

    assertEquals(
        "http://rs.tdwg.org/ac/terms/Multimedia", result.path(Indexing.EXTENSIONS).get(0).asText());

    JsonNode gadm = result.path("gadm");
    assertEquals("XAA_1", gadm.get("level0Gid").asText());
    assertEquals("XAA.1_1", gadm.get("level1Gid").asText());
    assertEquals("XAA.1.2_1", gadm.get("level2Gid").asText());
    assertEquals("XAA.1.3_1", gadm.get("level3Gid").asText());
    assertEquals("Countryland", gadm.get("level0Name").asText());
    assertEquals("Countyshire", gadm.get("level1Name").asText());
    assertEquals("Muni Cipality", gadm.get("level2Name").asText());
    assertEquals("Level 3 Cipality", gadm.get("level3Name").asText());
    assertEquals(4, gadm.path("gids").size());

    assertEquals(15, result.path("all").size());

    String expectedVerbatim =
        "{\"core\":{\"http://rs.tdwg.org/dwc/terms/eventID\":\"eventId\",\"http://rs.tdwg.org/dwc/terms/organismID\":"
            + "\"organismID\",\"http://rs.tdwg.org/dwc/terms/collectionCode\":\"collectionCode\","
            + "\"http://rs.tdwg.org/dwc/terms/taxonID\":\"taxonID\",\"http://rs.gbif.org/terms/1.0/projectId\":\"mv;à1|mv2\","
            + "\"http://rs.tdwg.org/dwc/terms/recordNumber\":\"recordNumber\","
            + "\"http://rs.tdwg.org/dwc/terms/parentEventID\":\"parentEventId\","
            + "\"http://rs.tdwg.org/dwc/terms/occurrenceID\":\"occurrenceID\",\"http://rs.tdwg.org/dwc/terms/locality\":"
            + "\"something:{something}\",\"http://purl.org/dc/terms/remark\":\"{\\\"something\\\":1}{\\\"something\\\":1}\","
            + "\"http://rs.tdwg.org/dwc/terms/catalogNumber\":\"catalogNumber\",\"http://rs.tdwg.org/dwc/terms/footprintWKT\":"
            + "\"footprintWKTfootprintWKTfootprintWKT\",\"http://rs.tdwg.org/dwc/terms/institutionCode\":\"institutionCode\","
            + "\"http://rs.tdwg.org/dwc/terms/recordedBy\":\"mv;à1|mv2\",\"http://rs.tdwg.org/dwc/terms/scientificName\":"
            + "\"scientificName\"},\"coreId\":null,\"extensions\":{\"http://rs.tdwg.org/ac/terms/Multimedia\":[{\"k\":\"v\"}]}}";
    assertEquals(expectedVerbatim, result.path("verbatim").toString());

    String expectedGbifClassification =
        "{\"acceptedUsage\":{\"key\":11,\"guid\":null,\"name\":\"accepted usage\",\"rank\":\"SPECIES\"},"
            + "\"classification\":[{\"key\":1,\"guid\":null,\"name\":\"KINGDOM\",\"rank\":\"KINGDOM\"},{\"key\":2,"
            + "\"guid\":null,\"name\":\"PHYLUM\",\"rank\":\"PHYLUM\"},{\"key\":3,\"guid\":null,\"name\":\"CLASS\","
            + "\"rank\":\"CLASS\"},{\"key\":4,\"guid\":null,\"name\":\"ORDER\",\"rank\":\"ORDER\"},{\"key\":5,"
            + "\"guid\":null,\"name\":\"FAMILY\",\"rank\":\"FAMILY\"},{\"key\":6,\"guid\":null,\"name\":\"GENUS\","
            + "\"rank\":\"GENUS\"},{\"key\":7,\"guid\":null,\"name\":\"SPECIES\",\"rank\":\"SPECIES\"}],"
            + "\"classificationPath\":\"_1_2_3_4_5_6\",\"diagnostics\":{\"matchType\":\"EXACT\",\"note\":\"note\","
            + "\"status\":\"ACCEPTED\"},\"kingdom\":\"KINGDOM\",\"kingdomKey\":\"1\",\"phylum\":\"PHYLUM\","
            + "\"phylumKey\":\"2\",\"classKey\":\"3\",\"order\":\"ORDER\",\"orderKey\":\"4\",\"family\":\"FAMILY\","
            + "\"familyKey\":\"5\",\"genus\":\"GENUS\",\"genusKey\":\"6\",\"species\":\"SPECIES\","
            + "\"speciesKey\":\"7\",\"synonym\":true,\"taxonID\":\"taxonID\","
            + "\"taxonKey\":[\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"10\",\"11\"],\"usage\":{\"key\":10,"
            + "\"guid\":null,\"name\":\"synonym\",\"rank\":\"SPECIES\"},\"usageParsedName\":{\"abbreviated\":false,"
            + "\"autonym\":false,\"basionymAuthorship\":{\"authors\":[\"setBasionymAuthorship\"],\"exAuthors\":["
            + "\"setBasionymAuthorship\"],\"empty\":true,\"year\":\"2000\"},\"binomial\":false,\"candidatus\":false,"
            + "\"code\":\"BACTERIAL\",\"combinationAuthorship\":{\"authors\":[\"setCombinationAuthorship\"],"
            + "\"exAuthors\":[\"setCombinationAuthorship\"],\"empty\":false,\"year\":\"2020\"},\"doubtful\":false,"
            + "\"genericName\":\"setGenus\",\"genus\":\"setGenus\",\"incomplete\":false,\"indetermined\":false,"
            + "\"infraspecificEpithet\":\"infraspecificEpithet\",\"notho\":\"GENERIC\",\"rank\":\"ABERRATION\","
            + "\"specificEpithet\":\"specificEpithet\",\"state\":\"COMPLETE\",\"terminalEpithet\":\"terminalEpithet\","
            + "\"trinomial\":false,\"type\":\"HYBRID_FORMULA\",\"uninomial\":\"setUninomial\"},"
            + "\"verbatimScientificName\":\"scientificName\",\"iucnRedListCategoryCode\":\"setIucnRedListCategoryCode\","
            + "\"class\":\"CLASS\"}";

    assertEquals(expectedGbifClassification, result.path("gbifClassification").toString());

    assertEquals("111", result.path("gbifId").asText());
    assertEquals("2.0", result.path("sampleSizeValue").asText());
    assertEquals("SampleSizeUnit", result.path("sampleSizeUnit").asText());
    assertEquals("2.0", result.path("organismQuantity").asText());
    assertEquals("OrganismQuantityType", result.path("organismQuantityType").asText());
    assertEquals("0.001", result.path("relativeOrganismQuantity").asText());
    assertEquals(
        "[{\"type\":\"OTHER\",\"value\":\"someId\"}]", result.path("identifiedByIds").toString());
    assertEquals(
        "[{\"type\":\"OTHER\",\"value\":\"someId\"}]", result.path("recordedByIds").toString());
    assertEquals("PRESENT", result.path("occurrenceStatus").asText());
    assertEquals("10.0", result.path("distanceFromCentroidInMeters").asText());

    assertEquals(institutionMatch.getKey(), result.path("institutionKey").asText());

    String expectedIssues =
        "[\"BASIS_OF_RECORD_INVALID\",\"INSTITUTION_MATCH_FUZZY\",\"ZERO_COORDINATE\"]";
    assertEquals(expectedIssues, result.path(Indexing.ISSUES).toString());
    assertEquals(
        OccurrenceIssue.values().length - expectedIssues.split(",").length,
        result.path(Indexing.NOT_ISSUES).size());
    assertEquals("2019-04-16T22:37:55.758", result.path(Indexing.CREATED).asText());

    // Vocabulary
    assertEquals(
        "{\"concept\":\"bla1\",\"lineage\":[\"bla1_1\"]}", result.path("lifeStage").toString());
    assertEquals(
        "{\"concept\":\"bla3\",\"lineage\":[\"bla3_1\"]}",
        result.path("establishmentMeans").toString());
    assertEquals(
        "{\"concept\":\"bla2\",\"lineage\":[\"bla2_1\"]}", result.path("pathway").toString());
    assertEquals(
        "{\"concept\":\"bla4\",\"lineage\":[\"bla4_1\"]}",
        result.path("degreeOfEstablishment").toString());
  }

  @Test
  public void converterEmptyRecordsTest() throws Exception {
    // State
    MetadataRecord mr = MetadataRecord.newBuilder().setLicense("setLicense").setId("777").build();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").build();
    ClusteringRecord cr = ClusteringRecord.newBuilder().setId("777").build();
    IdentifierRecord id = IdentifierRecord.newBuilder().setId("777").setInternalId("1").build();
    BasicRecord br = BasicRecord.newBuilder().setId("777").build();
    TemporalRecord tmr = TemporalRecord.newBuilder().setId("777").build();
    LocationRecord lr = LocationRecord.newBuilder().setId("777").build();
    TaxonRecord tr = TaxonRecord.newBuilder().setId("777").build();
    GrscicollRecord gr = GrscicollRecord.newBuilder().setId("777").build();
    MultimediaRecord mmr = MultimediaRecord.newBuilder().setId("777").build();

    // When
    String json =
        OccurrenceJsonConverter.builder()
            .basic(br)
            .identifier(id)
            .clustering(cr)
            .metadata(mr)
            .verbatim(er)
            .temporal(tmr)
            .location(lr)
            .taxon(tr)
            .grscicoll(gr)
            .multimedia(mmr)
            .build()
            .toJsonWithNulls();

    JsonNode result = MAPPER.readTree(json);

    // Should
    assertTrue(JsonValidationUtils.isValid(result.toString()));
    assertEquals("setLicense", result.get("license").asText());
  }
}
