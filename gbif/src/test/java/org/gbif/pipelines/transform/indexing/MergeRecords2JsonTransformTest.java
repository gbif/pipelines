package org.gbif.pipelines.transform.indexing;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.io.avro.location.LocationRecord;
import org.gbif.pipelines.io.avro.multimedia.MultimediaRecord;
import org.gbif.pipelines.io.avro.taxon.TaxonRecord;
import org.gbif.pipelines.io.avro.temporal.TemporalRecord;

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
public class MergeRecords2JsonTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {

    // State
    String id = "1a52cef5-f4cf-43f4-8fdf-4b4951515688";

    MetadataRecord metadata = MetadataRecord.newBuilder().setDatasetId(id).build();
    ExtendedRecord extended = ExtendedRecord.newBuilder().setId(id).build();
    InterpretedExtendedRecord interpreted =
        InterpretedExtendedRecord.newBuilder().setId(id).build();
    TemporalRecord temporal = TemporalRecord.newBuilder().setId(id).build();
    LocationRecord location = LocationRecord.newBuilder().setId(id).build();
    TaxonRecord taxon = TaxonRecord.newBuilder().setId(id).build();
    MultimediaRecord multimedia = MultimediaRecord.newBuilder().setId(id).build();

    PCollection<ExtendedRecord> verbatimCollections = p.apply(Create.of(extended));

    PCollection<KV<String, MetadataRecord>> metadataCollection =
        p.apply("MetadataRecord", Create.of(Collections.singletonMap(id, metadata)));

    PCollection<KV<String, InterpretedExtendedRecord>> interpretedCollection =
        p.apply("InterpretedExtendedRecord", Create.of(Collections.singletonMap(id, interpreted)));

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("TemporalRecord", Create.of(Collections.singletonMap(id, temporal)));

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("LocationRecord", Create.of(Collections.singletonMap(id, location)));

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("TaxonRecord", Create.of(Collections.singletonMap(id, taxon)));

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("MultimediaRecord", Create.of(Collections.singletonMap(id, multimedia)));

    // Expected
    final String expectedJson =
        "{\"datasetId\":\"1a52cef5-f4cf-43f4-8fdf-4b4951515688\",\"location\":{},\"id\":\"1a52cef5-f4cf-43f4-8fdf-4b4951515688\",\"verbatim\":{}}";
    final List<String> dataExpected = Collections.singletonList(expectedJson);

    // When
    MergeRecords2JsonTransform jsonTransform =
        MergeRecords2JsonTransform.create().withAvroCoders(p);

    PCollectionTuple tuple =
        PCollectionTuple.of(jsonTransform.getExtendedRecordTag(), verbatimCollections)
            .and(jsonTransform.getInterKvTag(), interpretedCollection)
            .and(jsonTransform.getLocationKvTag(), locationCollection)
            .and(jsonTransform.getMultimediaKvTag(), multimediaCollection)
            .and(jsonTransform.getTaxonomyKvTag(), taxonCollection)
            .and(jsonTransform.getTemporalKvTag(), temporalCollection)
            .and(jsonTransform.getMetadataKvTag(), metadataCollection);

    PCollection<String> resultCollection = tuple.apply(jsonTransform);

    // Should
    PAssert.that(resultCollection).containsInAnyOrder(dataExpected);
    p.run();
  }
}
