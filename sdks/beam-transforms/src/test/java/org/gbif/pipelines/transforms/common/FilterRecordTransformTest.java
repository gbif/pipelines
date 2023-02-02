package org.gbif.pipelines.transforms.common;

import java.util.Collections;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.IdentifierRecord;
import org.gbif.pipelines.transforms.core.VerbatimTransform;
import org.gbif.pipelines.transforms.specific.GbifIdTransform;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class FilterRecordTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void filterTest() {

    // State
    String id = "777";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreTerms(Collections.singletonMap("map", "value"))
            .build();
    IdentifierRecord ir =
        IdentifierRecord.newBuilder().setId(id).setInternalId("1").setFirstLoaded(1L).build();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    GbifIdTransform gbifIdTransform = GbifIdTransform.builder().create();

    // When
    PCollection<KV<String, ExtendedRecord>> erKv =
        p.apply("Read ExtendedRecord", Create.of(er))
            .apply("KV ExtendedRecord", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> irKv =
        p.apply("Read IdentifierRecord", Create.of(ir))
            .apply("KV IdentifierRecord", gbifIdTransform.toKv());

    FilterRecordsTransform filterRecordsTransform =
        FilterRecordsTransform.create(verbatimTransform.getTag(), gbifIdTransform.getTag());

    PCollection<ExtendedRecord> result =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), erKv)
            .and(gbifIdTransform.getTag(), irKv)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Filter verbatim", filterRecordsTransform.filter());

    // Should
    PAssert.that(result).containsInAnyOrder(er);
    p.run();
  }

  @Test
  public void filterEmptyGbifIdTest() {

    // State
    String id = "777";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(id)
            .setCoreTerms(Collections.singletonMap("map", "value"))
            .build();
    IdentifierRecord ir = IdentifierRecord.newBuilder().setId(id).setFirstLoaded(1L).build();

    VerbatimTransform verbatimTransform = VerbatimTransform.create();
    GbifIdTransform gbifIdTransform = GbifIdTransform.builder().create();

    // When
    PCollection<KV<String, ExtendedRecord>> erKv =
        p.apply("Read ExtendedRecord", Create.of(er))
            .apply("KV ExtendedRecord", verbatimTransform.toKv());

    PCollection<KV<String, IdentifierRecord>> irKv =
        p.apply("Read IdentifierRecord", Create.of(ir))
            .apply("KV IdentifierRecord", gbifIdTransform.toKv());

    FilterRecordsTransform filterRecordsTransform =
        FilterRecordsTransform.create(verbatimTransform.getTag(), gbifIdTransform.getTag());

    PCollection<ExtendedRecord> result =
        KeyedPCollectionTuple
            // Core
            .of(verbatimTransform.getTag(), erKv)
            .and(gbifIdTransform.getTag(), irKv)
            // Apply
            .apply("Grouping objects", CoGroupByKey.create())
            .apply("Filter verbatim", filterRecordsTransform.filter());

    // Should
    PAssert.that(result).empty();
    p.run();
  }
}
