package org.gbif.pipelines.transforms.table;

import java.util.ArrayList;
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
import org.gbif.pipelines.io.avro.DnaDerivedData;
import org.gbif.pipelines.io.avro.DnaDerivedDataRecord;
import org.gbif.pipelines.io.avro.EventCoreRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
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
import org.gbif.pipelines.transforms.core.MultiTaxonomyTransform;
import org.gbif.pipelines.transforms.core.TemporalTransform;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.extension.AudubonTransform;
import org.gbif.pipelines.transforms.extension.DnaDerivedDataTransform;
import org.gbif.pipelines.transforms.extension.HumboldtTransform;
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
    MultiTaxonRecord mtxr =
        MultiTaxonRecord.newBuilder()
            .setId("777")
            .setCoreId("setCoreId")
            .setTaxonRecords(new ArrayList<>())
            .build();
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
    DnaDerivedDataRecord dnar =
        DnaDerivedDataRecord.newBuilder()
            .setId("777")
            .setDnaDerivedDataItems(
                Collections.singletonList(
                    DnaDerivedData.newBuilder().setDnaSequenceID("foo1").build()))
            .build();
    HumboldtRecord hr = HumboldtRecord.newBuilder().setId("777").build();

    BasicTransform basicTransform = BasicTransform.builder().create();
    GbifIdTransform idTransform = GbifIdTransform.builder().create();
    ClusteringTransform clusteringTransform = ClusteringTransform.builder().create();
    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    TemporalTransform temporalTransform = TemporalTransform.builder().create();
    MultiTaxonomyTransform multiTaxonomyTransform = MultiTaxonomyTransform.builder().create();
    GrscicollTransform grscicollTransform = GrscicollTransform.builder().create();
    LocationTransform locationTransform = LocationTransform.builder().create();
    EventCoreTransform eventCoreTransform = EventCoreTransform.builder().create();

    // Extension
    MultimediaTransform multimediaTransform = MultimediaTransform.builder().create();
    AudubonTransform audubonTransform = AudubonTransform.builder().create();
    ImageTransform imageTransform = ImageTransform.builder().create();
    DnaDerivedDataTransform dnaDerivedDataTransform = DnaDerivedDataTransform.builder().create();
    HumboldtTransform humboldtTransform = HumboldtTransform.builder().create();

    PCollectionView<MetadataRecord> metadataView =
        p.apply("Read Metadata", Create.of(mr)).apply("Convert to view", View.asSingleton());

    OccurrenceHdfsRecordTransform transform =
        OccurrenceHdfsRecordTransform.builder()
            .extendedRecordTag(verbatimTransform.getTag())
            .identifierRecordTag(idTransform.getTag())
            .basicRecordTag(basicTransform.getTag())
            .clusteringRecordTag(clusteringTransform.getTag())
            .temporalRecordTag(temporalTransform.getTag())
            .multiTaxonRecordTag(multiTaxonomyTransform.getTag())
            .grscicollRecordTag(grscicollTransform.getTag())
            .locationRecordTag(locationTransform.getTag())
            .eventCoreRecordTag(eventCoreTransform.getTag())
            .multimediaRecordTag(multimediaTransform.getTag())
            .audubonRecordTag(audubonTransform.getTag())
            .imageRecordTag(imageTransform.getTag())
            .dnaRecordTag(dnaDerivedDataTransform.getTag())
            .humboldtRecordTag(humboldtTransform.getTag())
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

    PCollection<KV<String, MultiTaxonRecord>> multiTaxonomyCollection =
        p.apply("Create multi taxon", Create.of(mtxr))
            .apply("KV multi taxon", multiTaxonomyTransform.toKv());

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

    PCollection<KV<String, DnaDerivedDataRecord>> dnaCollection =
        p.apply("Create DNA", Create.of(dnar)).apply("KV DNA", dnaDerivedDataTransform.toKv());

    PCollection<KV<String, AudubonRecord>> audubonCollection =
        p.apply("Create audubon", Create.of(aur)).apply("KV audubon", audubonTransform.toKv());

    PCollection<KV<String, HumboldtRecord>> humboldtCollection =
        p.apply("Create humboldt", Create.of(hr)).apply("KV humboldt", humboldtTransform.toKv());

    PCollection<OccurrenceHdfsRecord> result =
        KeyedPCollectionTuple
            // Core
            .of(idTransform.getTag(), idCollection)
            .and(verbatimTransform.getTag(), verbatimCollection)
            .and(clusteringTransform.getTag(), clusteringCollection)
            .and(basicTransform.getTag(), basicCollection)
            .and(temporalTransform.getTag(), temporalCollection)
            .and(locationTransform.getTag(), locationCollection)
            .and(multiTaxonomyTransform.getTag(), multiTaxonomyCollection)
            .and(grscicollTransform.getTag(), grscicollCollection)
            .and(eventCoreTransform.getTag(), eventCoreCollection)
            .and(multimediaTransform.getTag(), multimediaCollection)
            .and(imageTransform.getTag(), imageCollection)
            .and(dnaDerivedDataTransform.getTag(), dnaCollection)
            .and(audubonTransform.getTag(), audubonCollection)
            .and(humboldtTransform.getTag(), humboldtCollection)
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
    expected.setTypestatus(null);
    expected.setSamplingprotocol(Collections.emptyList());
    expected.setHighergeography(Collections.emptyList());
    expected.setGeoreferencedby(Collections.emptyList());
    expected.setCollectionkey("setCollectionMatchKey");
    expected.setDatasettitle("setDatasetTitle");
    expected.setProjectid(Collections.singletonList("setProjectId"));
    expected.setAssociatedsequences(Collections.emptyList());
    expected.setDay(25);
    expected.setDnasequenceid(Collections.singletonList("foo1"));
    expected.setClassifications(new HashMap<>());
    expected.setNontaxonomicissue(List.of());

    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }
}
