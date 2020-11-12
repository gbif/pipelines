package org.gbif.pipelines.transforms.common;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class UniqueGbifIdTransformTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void skipTransformTest() {
    // State
    final List<BasicRecord> input = createCollection("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create(true);
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).containsInAnyOrder(input);
    PAssert.that(invalid).empty();
    p.run();
  }

  @Test
  public void emptyGbifIdTest() {
    // State
    final List<BasicRecord> input = createCollection("1");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(invalid).containsInAnyOrder(input);
    PAssert.that(normal).empty();
    p.run();
  }

  @Test
  public void withoutDuplicatesTest() {
    // State
    final List<BasicRecord> input = createCollection("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");
    final List<BasicRecord> expected = createCollection("1_1", "2_2", "3_3", "4_4", "5_5", "6_6");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).containsInAnyOrder(expected);
    PAssert.that(invalid).empty();
    p.run();
  }

  @Test
  public void allDuplicatesTest() {
    // State
    final List<BasicRecord> input = createCollection("1_1", "2_1", "3_1", "4_1", "5_1", "6_1");
    final List<BasicRecord> expectedNormal = createCollection("4_1"); // Has lowest SHA1 hash
    final List<BasicRecord> expectedInvalid = createCollection("1_1", "2_1", "3_1", "5_1", "6_1");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).containsInAnyOrder(expectedNormal);
    PAssert.that(invalid).containsInAnyOrder(expectedInvalid);
    p.run();
  }

  @Test
  public void noGbifIdTest() {
    // State
    final List<BasicRecord> input = createCollection("1", "2", "3", "4", "5", "6");
    final List<BasicRecord> expectedInvalid = createCollection("1", "2", "3", "4", "5", "6");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).empty();
    PAssert.that(invalid).containsInAnyOrder(expectedInvalid);
    p.run();
  }

  @Test
  public void allEqualTest() {
    // State
    final List<BasicRecord> input = createCollection("1_1", "1_1", "1_1", "1_1", "1_1", "1_1");
    final List<BasicRecord> expected = createCollection("1_1");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).containsInAnyOrder(expected);
    PAssert.that(invalid).empty();
    p.run();
  }

  @Test
  public void oneValueTest() {
    // Statet
    final List<BasicRecord> input = createCollection("1_1");
    final List<BasicRecord> expected = createCollection("1_1");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).containsInAnyOrder(expected);
    PAssert.that(invalid).empty();
    p.run();
  }

  @Test
  public void oneWithoutGbifIdValueTest() {
    // State
    final List<BasicRecord> input = createCollection("1");
    final List<BasicRecord> expected = createCollection("1");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).empty();
    PAssert.that(invalid).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void mixedValuesTest() {
    // State
    final List<BasicRecord> input = createCollection("1", "1_1", "2_2", "3_3", "4_1", "5", "6_6");
    final List<BasicRecord> expected = createCollection("2_2", "3_3", "4_1", "6_6");
    final List<BasicRecord> expectedInvalid = createCollection("1", "1_1", "5");

    // When
    UniqueGbifIdTransform transform = UniqueGbifIdTransform.create();
    PCollectionTuple tuple = p.apply(Create.of(input)).apply(transform);
    PCollection<BasicRecord> normal = tuple.get(transform.getTag());
    PCollection<BasicRecord> invalid = tuple.get(transform.getInvalidTag());

    // Should
    PAssert.that(normal).containsInAnyOrder(expected);
    PAssert.that(invalid).containsInAnyOrder(expectedInvalid);
    p.run();
  }

  private List<BasicRecord> createCollection(String... idName) {
    return Arrays.stream(idName)
        .map(
            x -> {
              String[] array = x.split("_");
              return BasicRecord.newBuilder()
                  .setId(array[0])
                  .setGbifId(array.length > 1 ? Long.valueOf(array[1]) : null)
                  .build();
            })
        .collect(Collectors.toList());
  }
}
