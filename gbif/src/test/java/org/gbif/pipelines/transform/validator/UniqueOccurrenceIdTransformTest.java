package org.gbif.pipelines.transform.validator;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * TODO: More tests to cover positive and negative cases
 */
@RunWith(JUnit4.class)
public class UniqueOccurrenceIdTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testFilterDuplicateObjectsByOccurrenceId() {
    //State
    final List<ExtendedRecord> input = createCollection("0001", "0001", "0002", "0003", "0004");
    final List<ExtendedRecord> expected = createCollection("0002", "0003", "0004");

    //When
    UniqueOccurrenceIdTransform transformationStream = UniqueOccurrenceIdTransform.create().withAvroCoders(p);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(input));

    PCollectionTuple tuple = inputStream.apply(transformationStream);

    PCollection<ExtendedRecord> collectionStream = tuple.get(transformationStream.getDataTag());

    //Should
    PAssert.that(collectionStream).containsInAnyOrder(expected);
    p.run();
  }

  private List<ExtendedRecord> createCollection(String... occurrenceIds) {
    return Arrays.stream(occurrenceIds)
      .map(x -> ExtendedRecord.newBuilder().setId(x).build())
      .collect(Collectors.toList());
  }

}