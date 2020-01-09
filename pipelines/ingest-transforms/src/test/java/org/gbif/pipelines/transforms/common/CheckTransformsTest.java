package org.gbif.pipelines.transforms.common;

import java.util.HashSet;

import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType;

import org.junit.Assert;
import org.junit.Test;

public class CheckTransformsTest {

  @Test
  public void checkRecordTypeAllValueTest() {

    // State
    HashSet<String> set = new HashSet<>();
    set.add(RecordType.ALL.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void checkRecordTypeMatchValueTest() {

    // State
    HashSet<String> set = new HashSet<>();
    set.add(RecordType.BASIC.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertTrue(result);
  }

  @Test
  public void checkRecordTypeMatchManyValueTest() {

    // State
    HashSet<String> set = new HashSet<>();
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
    HashSet<String> set = new HashSet<>();
    set.add(RecordType.AMPLIFICATION.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertFalse(result);
  }

  @Test
  public void checkRecordTypeMismatchManyValueTest() {

    // State
    HashSet<String> set = new HashSet<>();
    set.add(RecordType.AMPLIFICATION.name());
    set.add(RecordType.IMAGE.name());

    // When
    boolean result = CheckTransforms.checkRecordType(set, RecordType.BASIC, RecordType.AUDUBON);

    // Should
    Assert.assertFalse(result);
  }

}
