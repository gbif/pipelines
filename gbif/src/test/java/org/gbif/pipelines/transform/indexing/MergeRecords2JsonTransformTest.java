package org.gbif.pipelines.transform.indexing;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.InterpretedExtendedRecord;
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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
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

    ExtendedRecord extended = ExtendedRecord.newBuilder().setId(id).build();
    InterpretedExtendedRecord interpreted =
        InterpretedExtendedRecord.newBuilder().setId(id).build();
    TemporalRecord temporal = TemporalRecord.newBuilder().setId(id).build();
    LocationRecord location = LocationRecord.newBuilder().setId(id).build();
    TaxonRecord taxon = TaxonRecord.newBuilder().setId(id).build();
    MultimediaRecord multimedia = MultimediaRecord.newBuilder().setId(id).build();

    List<ExtendedRecord> extendedList = Collections.singletonList(extended);
    List<InterpretedExtendedRecord> interpretedList = Collections.singletonList(interpreted);
    List<TemporalRecord> temporalList = Collections.singletonList(temporal);
    List<LocationRecord> locationList = Collections.singletonList(location);
    List<TaxonRecord> taxonList = Collections.singletonList(taxon);
    List<MultimediaRecord> multimediaList = Collections.singletonList(multimedia);

    PCollection<ExtendedRecord> verbatimCollections = p.apply(Create.of(extendedList));

    PCollection<KV<String, InterpretedExtendedRecord>> interpretedCollection =
        p.apply("InterpretedExtendedRecord", Create.of(interpretedList))
            .apply(
                "InterpretedExtendedRecord map",
                MapElements.into(new TypeDescriptor<KV<String, InterpretedExtendedRecord>>() {})
                    .via((InterpretedExtendedRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, TemporalRecord>> temporalCollection =
        p.apply("TemporalRecord", Create.of(temporalList))
            .apply(
                "TemporalRecord map",
                MapElements.into(new TypeDescriptor<KV<String, TemporalRecord>>() {})
                    .via((TemporalRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, LocationRecord>> locationCollection =
        p.apply("LocationRecord", Create.of(locationList))
            .apply(
                "LocationRecord map",
                MapElements.into(new TypeDescriptor<KV<String, LocationRecord>>() {})
                    .via((LocationRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, TaxonRecord>> taxonCollection =
        p.apply("TaxonRecord", Create.of(taxonList))
            .apply(
                "TaxonRecord map",
                MapElements.into(new TypeDescriptor<KV<String, TaxonRecord>>() {})
                    .via((TaxonRecord t) -> KV.of(t.getId(), t)));

    PCollection<KV<String, MultimediaRecord>> multimediaCollection =
        p.apply("MultimediaRecord", Create.of(multimediaList))
            .apply(
                "MultimediaRecord map",
                MapElements.into(new TypeDescriptor<KV<String, MultimediaRecord>>() {})
                    .via((MultimediaRecord t) -> KV.of(t.getId(), t)));

    // Expected
    final String expectedJson =
        "{\"location\":{},\"id\":\"1a52cef5-f4cf-43f4-8fdf-4b4951515688\",\"verbatim\":{}}";
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
            .and(jsonTransform.getTemporalKvTag(), temporalCollection);

    PCollection<String> resultCollection = tuple.apply(jsonTransform);

    // Should
    PAssert.that(resultCollection).containsInAnyOrder(dataExpected);
    p.run();
  }
}
