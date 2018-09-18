package org.gbif.pipelines.transforms;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UniqueIdTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void filterDuplicateObjectsByOccurrenceIdTest() {
    // State
    final List<ExtendedRecord> input = createCollection("0001", "0001", "0002", "0003", "0004");
    final List<ExtendedRecord> expected = createCollection("0002", "0003", "0004");

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(UniqueIdTransform.create());

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  private List<ExtendedRecord> createCollection(String... occurrenceIds) {
    return Arrays.stream(occurrenceIds)
        .map(x -> ExtendedRecord.newBuilder().setId(x).build())
        .collect(Collectors.toList());
  }
}
