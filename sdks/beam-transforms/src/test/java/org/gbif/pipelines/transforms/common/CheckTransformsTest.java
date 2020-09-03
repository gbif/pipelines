package org.gbif.pipelines.transforms.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class CheckTransformsTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void checkRecordTypeAllValueTest() {

    // State
    Set<String> set = Collections.singleton(RecordType.ALL.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void checkRecordTypeMatchValueTest() {

    // State
    Set<String> set = Collections.singleton(RecordType.BASIC.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void checkRecordTypeMatchManyValueTest() {

    // State
    Set<String> set = new HashSet<>();
    set.add(RecordType.BASIC.name());
    set.add(RecordType.AUDUBON.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void checkRecordTypeMismatchOneValueTest() {

    // State
    Set<String> set = Collections.singleton(RecordType.AMPLIFICATION.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void checkRecordTypeMismatchManyValueTest() {

    // State
    Set<String> set = new HashSet<>();
    set.add(RecordType.AMPLIFICATION.name());
    set.add(RecordType.IMAGE.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void checkTrueConditionFlagTest() {

    // State
    final List<String> input = Collections.singletonList("something");

    // When
    CheckTransforms<String> transforms = CheckTransforms.create(String.class, true);
    PCollection<String> apply = p.apply(Create.of(input)).apply(transforms);

    // Should
    PAssert.that(apply).containsInAnyOrder(input);
    p.run();
  }

  @Test
  public void checkFalseConditionFlagTest() {

    // State
    final List<String> input = Collections.singletonList("something");

    // When
    CheckTransforms<String> transforms = CheckTransforms.create(String.class, false);
    PCollection<String> apply = p.apply(Create.of(input)).apply(transforms);

    // Should
    PAssert.that(apply).empty();
    p.run();
  }
}
