package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
    Optional<ExtendedRecord> result = OccurrenceExtensionConverter.convert(coreMap, extMap);

    // Should
    Assert.assertTrue(result.isPresent());
    ExtendedRecord erResult = result.get();
    Assert.assertEquals(id, erResult.getId());
    Assert.assertEquals(somethingCore, erResult.getCoreTerms().get(somethingCore));
    Assert.assertEquals(somethingExt, erResult.getCoreTerms().get(somethingExt));
  }
}
