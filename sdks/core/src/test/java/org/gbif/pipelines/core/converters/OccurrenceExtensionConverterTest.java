package org.gbif.pipelines.core.converters;

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
    String idCore = "1";
    String idExt = "2";
    String somethingCore = "somethingCore";
    String somethingExt = "somethingExt";

    Map<String, String> coreMap = new HashMap<>(2);
    coreMap.put(DwcTerm.occurrenceID.qualifiedName(), idCore);
    coreMap.put(somethingCore, somethingCore);

    Map<String, String> extMap = new HashMap<>(2);
    extMap.put(DwcTerm.occurrenceID.qualifiedName(), idExt);
    extMap.put(somethingExt, somethingExt);

    // When
    Optional<ExtendedRecord> result = OccurrenceExtensionConverter.convert(coreMap, extMap);

    // Should
    Assert.assertTrue(result.isPresent());
    ExtendedRecord erResult = result.get();
    Assert.assertEquals(idExt, erResult.getId());
    Assert.assertEquals(somethingCore, erResult.getCoreTerms().get(somethingCore));
    Assert.assertEquals(somethingExt, erResult.getCoreTerms().get(somethingExt));
  }
}
