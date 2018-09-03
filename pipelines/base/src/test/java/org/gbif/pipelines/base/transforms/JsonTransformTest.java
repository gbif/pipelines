package org.gbif.pipelines.base.transforms;

import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {

    // State
    String id = "1a52cef5-f4cf-43f4-8fdf-4b4951515688";

    MetadataRecord mdr = MetadataRecord.newBuilder().setDatasetId(id).build();
    ExtendedRecord er = ExtendedRecord.newBuilder().setId(id).build();
    BasicRecord br = BasicRecord.newBuilder().setId(id).build();
    TemporalRecord tr = TemporalRecord.newBuilder().setId(id).build();
    LocationRecord lr = LocationRecord.newBuilder().setId(id).build();
    TaxonRecord txr = TaxonRecord.newBuilder().setId(id).build();
    MultimediaRecord mr = MultimediaRecord.newBuilder().setId(id).build();

    PCollection<ExtendedRecord> verbatimCollections = p.apply(Create.of(er));

    PCollection<KV<String, MetadataRecord>> metadataCollection =
        p.apply("MetadataRecord", Create.of(Collections.singletonMap(id, mdr)));

    PCollection<KV<String, BasicRecord>> interpretedCollection =
        p.apply("InterpretedExtendedRecord", Create.of(Collections.singletonMap(id, br)));

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("TemporalRecord", Create.of(Collections.singletonMap(id, tr)));

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("LocationRecord", Create.of(Collections.singletonMap(id, lr)));

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("TaxonRecord", Create.of(Collections.singletonMap(id, txr)));

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("MultimediaRecord", Create.of(Collections.singletonMap(id, mr)));

    // Expected
    final String expectedJson =
        "{\"datasetId\":\"1a52cef5-f4cf-43f4-8fdf-4b4951515688\",\"issues\":[],\"id\":\"1a52cef5-f4cf-43f4-8fdf-4b4951515688\",\"verbatim\":{}}";
    final List<String> dataExpected = Collections.singletonList(expectedJson);

    // When
    JsonTransform jsonTransform = JsonTransform.create();

    PCollection<String> result =
        PCollectionTuple.of(jsonTransform.getExtendedRecordTag(), verbatimCollections)
            .and(jsonTransform.getBasicKvTag(), interpretedCollection)
            .and(jsonTransform.getLocationKvTag(), locationCollection)
            .and(jsonTransform.getMultimediaKvTag(), multimediaCollection)
            .and(jsonTransform.getTaxonomyKvTag(), taxonCollection)
            .and(jsonTransform.getTemporalKvTag(), temporalCollection)
            .and(jsonTransform.getMetadataKvTag(), metadataCollection)
            .apply(jsonTransform);

    // Should
    PAssert.that(result).containsInAnyOrder(dataExpected);
    p.run();
  }
}
