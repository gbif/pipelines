package org.gbif.pipelines.transform;

import java.util.Collections;
import java.util.List;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class Kv2ValueTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTransformation() {

    // State
    KV<String, Integer> kv = KV.of("1", 1);

    // Expected
    List<Integer> expected = Collections.singletonList(1);

    // When
    PCollection<KV<String, Integer>> collection = p.apply(Create.of(kv));
    PCollection<Integer> result = collection.apply(Kv2Value.create());

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }
}
