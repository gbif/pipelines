package org.gbif.pipelines.transforms.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class UniqueIdTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void filterDuplicateDifferentTest() {
    // State
    final List<ExtendedRecord> input =
        createCollection("0001_1", "0001_1", "0001_2", "0002_1", "0003_1", "0004_1");
    final List<ExtendedRecord> expected = createCollection("0002_1", "0003_1", "0004_1");

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(UniqueIdTransform.create());

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void filterDuplicateIdenticalTest() {
    // State
    final List<ExtendedRecord> input =
        createCollection("0001_1", "0001_1", "0001_1", "0002_1", "0003_1", "0004_1");
    final List<ExtendedRecord> expected = createCollection("0001_1", "0002_1", "0003_1", "0004_1");

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(UniqueIdTransform.create());

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  private List<ExtendedRecord> createCollection(String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");
              return ExtendedRecord.newBuilder()
                  .setId(array[0])
                  .setCoreTerms(Collections.singletonMap("key", array[1]))
                  .build();
            })
        .collect(Collectors.toList());
  }
}
