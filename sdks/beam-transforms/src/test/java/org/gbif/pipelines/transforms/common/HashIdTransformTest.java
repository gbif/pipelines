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
public class HashIdTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void hashIdTest() {

    // State
    final String datasetId = "f349d447-1c92-4637-ab32-8ae559497032";
    final List<ExtendedRecord> input = createCollection("0001_1", "0002_2", "0003_3");
    final List<ExtendedRecord> expected =
        createCollection(
            "20d8ab138ab4c919cbf32f5d9e667812077a0ee4_1",
            "1122dc31ba32e386e3a36719699fdb5fb1d2912f_2",
            "f2b1c436ad680263d74bf1498bf7433d9bb4b31a_3");

    // When
    PCollection<ExtendedRecord> result =
        p.apply(Create.of(input)).apply(HashIdTransform.create(datasetId));

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
