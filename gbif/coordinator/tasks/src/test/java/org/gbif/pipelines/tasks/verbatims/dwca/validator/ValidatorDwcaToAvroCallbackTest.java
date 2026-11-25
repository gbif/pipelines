package org.gbif.pipelines.tasks.verbatims.dwca.validator;

import static org.gbif.api.model.pipelines.InterpretationType.RecordType.getAllValidatorInterpretationAsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class ValidatorDwcaToAvroCallbackTest {

  /** An archive with an unknown extension (null type) used to fail every orchestrated DwC-A. */
  @Test
  public void interpretTypesDropsUnknownExtensions() {
    Set<String> extensions = new HashSet<>(Arrays.asList("MULTIMEDIA", null));

    Set<String> types = ValidatorDwcaToAvroCallback.interpretTypes(extensions);

    assertTrue(types.contains("MULTIMEDIA"));
    assertTrue(types.containsAll(getAllValidatorInterpretationAsString()));
    assertFalse(types.stream().anyMatch(t -> t == null));
  }

  @Test
  public void interpretTypesWithoutExtensions() {
    Set<String> types = ValidatorDwcaToAvroCallback.interpretTypes(new HashSet<>());

    assertEquals(new HashSet<>(getAllValidatorInterpretationAsString()), new HashSet<>(types));
  }
}
