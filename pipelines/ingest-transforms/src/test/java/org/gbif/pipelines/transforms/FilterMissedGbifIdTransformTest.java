package org.gbif.pipelines.transforms;

import org.gbif.pipelines.core.interpreters.core.BasicInterpreter;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class FilterMissedGbifIdTransformTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void filterTest() {

    // State
    String one = "f349d447-1c92-4637-ab32-8ae559497032";
    String two = BasicInterpreter.GBIF_ID_INVALID;

    // When
    PCollection<String> result = p.apply(Create.of(one, two)).apply(FilterMissedGbifIdTransform.create());

    // Should
    PAssert.that(result).satisfies((SerializableFunction<Iterable<String>, Void>) input -> {
      input.forEach(x -> {
        if (x.contains(two)) {
          throw new IllegalArgumentException("Wrong value!");
        }
      });
      return null;
    });
    p.run();
  }
}
