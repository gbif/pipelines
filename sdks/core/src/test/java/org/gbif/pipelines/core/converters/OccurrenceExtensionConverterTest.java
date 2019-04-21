package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.junit.Assert;
import org.junit.Test;

public class OccurrenceExtensionConverterTest {

  @Test
  public void occurrenceAsExtensionTest() {
    // State
    String id = "1";
    String somethingCore = "somethingCore";
    String somethingExt = "somethingExt";

    Map<String, String> coreMap = Collections.singletonMap(somethingCore, somethingCore);

    Map<String, String> extMap = new HashMap<>(2);
    extMap.put(DwcTerm.occurrenceID.qualifiedName(), id);
    extMap.put(somethingExt, somethingExt);

    // When
    ExtendedRecord result = OccurrenceExtensionConverter.convert(coreMap, extMap);

    // Should
    Assert.assertNotNull(result);
    Assert.assertEquals(id, result.getId());
    Assert.assertEquals(somethingCore, result.getCoreTerms().get(somethingCore));
    Assert.assertEquals(somethingExt, result.getCoreTerms().get(somethingExt));

  }

}
