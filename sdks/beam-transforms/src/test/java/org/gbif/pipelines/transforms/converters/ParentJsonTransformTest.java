package org.gbif.pipelines.transforms.converters;

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
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.common.beam.coders.AvroKvCoder;
import org.gbif.pipelines.core.converters.ParentJsonConverter;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.GadmFeatures;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MachineTag;
import org.gbif.pipelines.io.avro.MeasurementOrFact;
import org.gbif.pipelines.io.avro.MeasurementOrFactRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.json.DerivedMetadataRecord;
import org.gbif.pipelines.io.avro.json.EventInheritedRecord;
import org.gbif.pipelines.io.avro.json.LocationInheritedRecord;
import org.gbif.pipelines.io.avro.json.TemporalInheritedRecord;
import org.gbif.pipelines.transforms.core.ConvexHullFn;
import org.gbif.pipelines.transforms.core.DerivedMetadataTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
import org.gbif.pipelines.transforms.core.InheritedFieldsTransform;
import org.gbif.pipelines.transforms.core.LocationTransform;
import org.gbif.pipelines.transforms.core.TemporalCoverageFn;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.ImageTransform;
import org.gbif.pipelines.transforms.extension.MeasurementOrFactTransform;
import org.gbif.pipelines.transforms.extension.MultimediaTransform;
import org.gbif.pipelines.transforms.specific.IdentifierTransform;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class ParentJsonTransformTest {

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

    Map<String, List<Map<String, String>>> exts = new HashMap<>(2);
    exts.put(DwcTerm.Occurrence.qualifiedName(), Collections.singletonList(erMap));
    exts.put(
        "http://rs.tdwg.org/ac/terms/Multimedia",
        Collections.singletonList(Collections.singletonMap("k", "v")));

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId("777")
            .setCoreRowType("core")
            .setCoreTerms(erMap)
            .setExtensions(exts)
            .build();

    EventCoreRecord ecr = EventCoreRecord.newBuilder().setId("777").build();

    IdentifierRecord ir =
        IdentifierRecord.newBuilder()
            .setId("777")
            .setFirstLoaded(1L)
            .setInternalId("888")
            .setUniqueKey("999")
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

    DerivedMetadataRecord dmr = DerivedMetadataRecord.newBuilder().setId("777").build();
    LocationInheritedRecord lir = LocationInheritedRecord.newBuilder().setId("777").build();
    TemporalInheritedRecord tir = TemporalInheritedRecord.newBuilder().setId("777").build();
    EventInheritedRecord eir = EventInheritedRecord.newBuilder().setId("777").build();

    MeasurementOrFactRecord mofr =
        MeasurementOrFactRecord.newBuilder()
            .setId("777")
            .setMeasurementOrFactItems(
                Collections.singletonList(
                    MeasurementOrFact.newBuilder()
                        .setMeasurementType("sampling")
                        .setMeasurementMethod("sample")
                        .build()))
            .build();

    // Core
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();
    IdentifierTransform identifierTransform = IdentifierTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();
    MeasurementOrFactTransform measurementOrFactTransform =
        MeasurementOrFactTransform.builder().create();

    // Derived metadata
    DerivedMetadataTransform derivedMetadataTransform =
        DerivedMetadataTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .convexHullTag(ConvexHullFn.tag())
            .temporalCoverageTag(TemporalCoverageFn.tag())
            .build();

    // When
    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", Create.of(mr)).apply("Convert to view", View.asSingleton());

    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Read Verbatim", Create.of(er))
            .apply("Map Verbatim to KV", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> identifierCollection =
        p.apply("Read identifiers", Create.of(ir))
            .apply("Map identifiers to KV", identifierTransform.toKv());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Read Event core", Create.of(ecr))
            .apply("Map Event core to KV", eventCoreTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Read Temporal", Create.of(tmr))
            .apply("Map Temporal to KV", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Read Location", Create.of(lr))
            .apply("Map Location to KV", locationTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Read Multimedia", Create.of(mmr))
            .apply("Map Multimedia to KV", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Read Image", Create.empty(new TypeDescriptor<ImageRecord>() {}))
            .apply("Map Image to KV", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Read Audubon", Create.empty(new TypeDescriptor<AudubonRecord>() {}))
            .apply("Map Audubon to KV", audubonTransform.toKv());

    PCollection<KV<String, DerivedMetadataRecord>> derivedMetadataCollection =
        p.apply("Read DerivedMetadataRecord", Create.of(dmr))
            .apply("Map DerivedMetadataRecord to KV", derivedMetadataTransform.toKv());

    PCollection<KV<String, LocationInheritedRecord>> locationInheritedCollection =
        p.apply("Read LocationInheritedRecord", Create.of(lir))
            .apply("Map LocationInheritedRecord to KV", WithKeys.of(LocationInheritedRecord::getId))
            .setCoder(AvroKvCoder.of(LocationInheritedRecord.class));

    PCollection<KV<String, TemporalInheritedRecord>> temporalInheritedCollection =
        p.apply("Read TemporalInheritedRecord", Create.of(tir))
            .apply("Map TemporalInheritedRecord to KV", WithKeys.of(TemporalInheritedRecord::getId))
            .setCoder(AvroKvCoder.of(TemporalInheritedRecord.class));

    PCollection<KV<String, EventInheritedRecord>> eventInheritedCollection =
        p.apply("Read EventInheritedRecord", Create.of(eir))
            .apply("Map EventInheritedRecord to KV", WithKeys.of(EventInheritedRecord::getId))
            .setCoder(AvroKvCoder.of(EventInheritedRecord.class));

    PCollection<KV<String, MeasurementOrFactRecord>> measurementOrFactCollection =
        p.apply("Read MeasurementOrFactRecord", Create.of(mofr))
            .apply("Map MeasurementOrFactRecord to KV", measurementOrFactTransform.toKv());

    SingleOutput<KV<String, CoGbkResult>, String> eventJsonDoFn =
        ParentJsonTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(identifierTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .derivedMetadataRecordTag(DerivedMetadataTransform.tag())
            .measurementOrFactRecordTag(measurementOrFactTransform.getTag())
            .locationInheritedRecordTag(InheritedFieldsTransform.LIR_TAG)
            .temporalInheritedRecordTag(InheritedFieldsTransform.TIR_TAG)
            .eventInheritedRecordTag(InheritedFieldsTransform.EIR_TAG)
            .metadataView(metadataView)
            .build()
            .converter();

    PCollection<String> jsonCollection =
        KeyedPCollectionTuple
            // Core
            .of(eventCoreTransform.getTag(), eventCoreCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            // Extension
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            .and(measurementOrFactTransform.getTag(), measurementOrFactCollection)
            // Internal
            .and(identifierTransform.getTag(), identifierCollection)
            // Raw
            .and(verbatimTransform.getTag(), verbatimCollection)
            // DerivedMetadata
            .and(DerivedMetadataTransform.tag(), derivedMetadataCollection)
            .and(InheritedFieldsTransform.LIR_TAG, locationInheritedCollection)
            .and(InheritedFieldsTransform.TIR_TAG, temporalInheritedCollection)
            .and(InheritedFieldsTransform.EIR_TAG, eventInheritedCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging to json", eventJsonDoFn);

    // Should
    String json =
        ParentJsonConverter.builder()
            .metadata(mr)
            .eventCore(ecr)
            .identifier(ir)
            .temporal(tmr)
            .location(lr)
            .multimedia(mmr)
            .verbatim(er)
            .derivedMetadata(dmr)
            .measurementOrFactRecord(mofr)
            .locationInheritedRecord(lir)
            .temporalInheritedRecord(tir)
            .eventInheritedRecord(eir)
            .build()
            .toJson();

    PAssert.that(jsonCollection).containsInAnyOrder(json);
    p.run();
  }
}
