package org.gbif.pipelines.transforms.converters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.model.collections.lookup.Match.MatchType;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.converters.OccurrenceJsonConverter;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.Authorship;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MachineTag;
import org.gbif.pipelines.io.avro.MediaType;
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
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.GrscicollTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.ClusteringTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class OccurrenceJsonTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void converterTest() {
    // State
    final String multivalue1 = "mv;à1";
    final String multivalue2 = "mv2";

    Map<String, String> erMap = new HashMap<>();
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
            .setProjectId("setProjectId")
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
            .setPreparations(Arrays.asList(multivalue1, multivalue2))
            .setSamplingProtocol(Arrays.asList(multivalue1, multivalue2))
            .setTypeStatus(Arrays.asList(TypeStatus.TYPE.name(), TypeStatus.TYPE_SPECIES.name()))
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
                        .setId("777")
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
            .setId("777")
            .setInstitutionMatch(institutionMatch)
            .setCollectionMatch(collectionMatch)
            .build();
    gr.getIssues().getIssueList().add(OccurrenceIssue.INSTITUTION_MATCH_FUZZY.name());

    // State
    Multimedia stillImage = new Multimedia();
    stillImage.setType(MediaType.StillImage.name());
    stillImage.setFormat("image/jpeg");
    stillImage.setLicense("somelicense");
    stillImage.setIdentifier("identifier1");
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
    movingImage.setIdentifier("identifier2");
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

    // Core
    BasicTransform basicTransform = BasicTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    GbifIdTransform gbifIdTransform = GbifIdTransform.builder().create();
    ClusteringTransform clusteringTransform = ClusteringTransform.builder().create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    // When
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", Create.of(mr)).apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", Create.of(er))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> idCollection =
        p.apply("Read GBIF ids", Create.of(id)).apply("Map GBIF ids to KV", gbifIdTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Read Basic", Create.of(br)).apply("Map Basic to KV", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", Create.of(tmr))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", Create.of(lr))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, ClusteringRecord>> clusteringCollection =
        p.apply("Read clustering", Create.of(cr))
            .apply("Map clustering to KV", clusteringTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("Read Taxon", Create.of(tr)).apply("Map Taxon to KV", taxonomyTransform.toKv());

    PCollection<KV<String, GrscicollRecord>> grscicollCollection =
        p.apply("Read Grscicoll", Create.of(gr))
            .apply("Map Grscicoll to KV", grscicollTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", Create.of(mmr))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", Create.empty(new TypeDescriptor<ImageRecord>() {}))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", Create.empty(new TypeDescriptor<AudubonRecord>() {}))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    SingleOutput<KV<String, CoGbkResult>, String> occurrenceJsonDoFn =
        OccurrenceJsonTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(gbifIdTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .clusteringRecordTag(clusteringTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .taxonRecordTag(taxonomyTransform.getTag())
            .grscicollRecordTag(grscicollTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<String> jsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(basicTransform.getTag(), basicCollection)
            .and(gbifIdTransform.getTag(), idCollection)
            .and(clusteringTransform.getTag(), clusteringCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(taxonomyTransform.getTag(), taxonCollection)
            .and(grscicollTransform.getTag(), grscicollCollection)
            // Extension
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", occurrenceJsonDoFn);

    // Should
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

    PAssert.that(jsonCollection).containsInAnyOrder(Collections.singletonList(json));
    p.run();
  }
}
