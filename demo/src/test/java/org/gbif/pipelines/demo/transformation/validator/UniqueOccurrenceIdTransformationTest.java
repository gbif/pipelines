package org.gbif.pipelines.demo.transformation.validator;

import org.gbif.pipelines.common.beam.Coders;
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
public class UniqueOccurrenceIdTransformationTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void Should_FilterDuplicateObjects_When_OccurrenceIdIsTheSame() {
    //State
    final List<ExtendedRecord> input = createCollection("0001", "0001", "0002", "0003", "0004");
    final List<ExtendedRecord> expected = createCollection("0002", "0003", "0004");

    //When
    Coders.registerAvroCoders(p, ExtendedRecord.class);

    PCollection<ExtendedRecord> inputStream = p.apply(Create.of(input));

    UniqueOccurrenceIdTransformation transformationStream = new UniqueOccurrenceIdTransformation();
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