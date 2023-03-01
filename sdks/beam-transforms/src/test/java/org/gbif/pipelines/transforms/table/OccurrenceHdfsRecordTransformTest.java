package org.gbif.pipelines.transforms.table;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ClusteringRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.io.avro.grscicoll.Match;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.gbif.pipelines.transforms.core.EventCoreTransform;
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
public class OccurrenceHdfsRecordTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void generalTest() {

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DwcTerm.measurementID.qualifiedName(), "Id1");
    ext1.put(DwcTerm.measurementType.qualifiedName(), "Type1");
    ext1.put(DwcTerm.measurementValue.qualifiedName(), "1.5");
    ext1.put(DwcTerm.measurementAccuracy.qualifiedName(), "Accurancy1");
    ext1.put(DwcTerm.measurementUnit.qualifiedName(), "Unit1");
    ext1.put(DwcTerm.measurementDeterminedBy.qualifiedName(), "By1");
    ext1.put(DwcTerm.measurementMethod.qualifiedName(), "Method1");
    ext1.put(DwcTerm.measurementRemarks.qualifiedName(), "Remarks1");
    ext1.put(DwcTerm.measurementDeterminedDate.qualifiedName(), "2010/2011");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MEASUREMENT_OR_FACT.getRowType(), Collections.singletonList(ext1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("777").setExtensions(ext).build();
    IdentifierRecord id =
        IdentifierRecord.newBuilder()
            .setId("777")
            .setInternalId("777")
            .setAssociatedKey("setTriplet")
            .build();
    MetadataRecord mr =
        MetadataRecord.newBuilder()
            .setId("777")
            .setDatasetTitle("setDatasetTitle")
            .setProjectId("setProjectId")
            .build();
    BasicRecord br =
        BasicRecord.newBuilder()
            .setId("777")
            .setDatasetName(Collections.singletonList("setDatasetName"))
            .build();
    ClusteringRecord cr = ClusteringRecord.newBuilder().setId("777").setIsClustered(true).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId("777").setDay(25).build();
    TaxonRecord txr = TaxonRecord.newBuilder().setId("777").setCoreId("setCoreId").build();
    GrscicollRecord gr =
        GrscicollRecord.newBuilder()
            .setId("777")
            .setCollectionMatch(Match.newBuilder().setKey("setCollectionMatchKey").build())
            .build();
    LocationRecord lr = LocationRecord.newBuilder().setId("777").setCountry("setCountry").build();
    EventCoreRecord ecr =
        EventCoreRecord.newBuilder()
            .setId("777")
            .setParentEventID("setParentEventID")
            .setCreated(new Date().getTime())
            .build();
    MultimediaRecord mmr = MultimediaRecord.newBuilder().setId("777").build();
    AudubonRecord aur = AudubonRecord.newBuilder().setId("777").build();
    ImageRecord imr = ImageRecord.newBuilder().setId("777").build();

    BasicTransform basicTransform = BasicTransform.builder().create();
    GbifIdTransform idTransform = GbifIdTransform.builder().create();
    ClusteringTransform clusteringTransform = ClusteringTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    TaxonomyTransform taxonomyTransform = TaxonomyTransform.builder().create();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", Create.of(mr)).apply("Convert to view", View.asSingleton());

    OccurrenceHdfsRecordTransform transform =
        OccurrenceHdfsRecordTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(idTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .clusteringRecordTag(clusteringTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .taxonRecordTag(taxonomyTransform.getTag())
            .grscicollRecordTag(grscicollTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .metadataView(metadataView)
            .build();

    // When
    PCollection<KV<String, ExtendedRecord>> verbatimCollection =
        p.apply("Create er", Create.of(er)).apply("KV er", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> idCollection =
        p.apply("Create id", Create.of(id)).apply("KV id", idTransform.toKv());

    PCollection<KV<String, ClusteringRecord>> clusteringCollection =
        p.apply("Create clustering", Create.of(cr))
            .apply("KV clustering", clusteringTransform.toKv());

    PCollection<KV<String, BasicRecord>> basicCollection =
        p.apply("Create basic", Create.of(br)).apply("KV basic", basicTransform.toKv());

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("Create temporal", Create.of(tr)).apply("KV temporal", temporalTransform.toKv());

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("Create location", Create.of(lr)).apply("KV location", locationTransform.toKv());

    PCollection<KV<String, TaxonRecord>> taxonomyCollection =
        p.apply("Create taxon", Create.of(txr)).apply("KV taxon", taxonomyTransform.toKv());

    PCollection<KV<String, GrscicollRecord>> grscicollCollection =
        p.apply("Create grscicoll", Create.of(gr)).apply("KV grscicoll", grscicollTransform.toKv());

    PCollection<KV<String, EventCoreRecord>> eventCoreCollection =
        p.apply("Create eventcore", Create.of(ecr))
            .apply("KV eventcore", eventCoreTransform.toKv());

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("Create multimedia", Create.of(mmr))
            .apply("KV multimedia", multimediaTransform.toKv());

    PCollection<KV<String, ImageRecord>> imageCollection =
        p.apply("Create image", Create.of(imr)).apply("KV image", imageTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Create audubon", Create.of(aur)).apply("KV audubon", audubonTransform.toKv());

    PCollection<OccurrenceHdfsRecord> result =
        KeyedPCollectionTuple
            // Core
            .of(idTransform.getTag(), idCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            .and(clusteringTransform.getTag(), clusteringCollection)
            .and(basicTransform.getTag(), basicCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(taxonomyTransform.getTag(), taxonomyCollection)
            .and(grscicollTransform.getTag(), grscicollCollection)
            .and(eventCoreTransform.getTag(), eventCoreCollection)
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Merging", transform.converter());

    // Should
    OccurrenceHdfsRecord expected = new OccurrenceHdfsRecord();
    expected.setGbifid("777");
    expected.setDatasetid(Collections.emptyList());
    expected.setDatasetname(Collections.singletonList("setDatasetName"));
    expected.setDwcaextension(
        Collections.singletonList("http://rs.tdwg.org/dwc/terms/MeasurementOrFact"));
    expected.setIsincluster(true);
    expected.setIdentifiedby(Collections.emptyList());
    expected.setIdentifiedbyid(Collections.emptyList());
    expected.setIssue(Collections.emptyList());
    expected.setMediatype(Collections.emptyList());
    expected.setNetworkkey(Collections.emptyList());
    expected.setRecordedby(Collections.emptyList());
    expected.setRecordedbyid(Collections.emptyList());
    expected.setPreparations(Collections.emptyList());
    expected.setOthercatalognumbers(Collections.emptyList());
    expected.setParenteventgbifid(Collections.emptyList());
    expected.setTypestatus(Collections.emptyList());
    expected.setSamplingprotocol(Collections.emptyList());
    expected.setCollectionkey("setCollectionMatchKey");
    expected.setDatasettitle("setDatasetTitle");
    expected.setProjectid(Collections.singletonList("setProjectId"));
    expected.setDay(25);

    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }
}
