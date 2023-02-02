package org.gbif.pipelines.core.converters;

import static org.gbif.dwc.terms.DwcTerm.MeasurementOrFact;
import static org.gbif.dwc.terms.DwcTerm.Occurrence;
import static org.gbif.dwc.terms.DwcTerm.ResourceRelationship;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    // Core
    Map<String, String> coreMap = new HashMap<>(2);
    coreMap.put(DwcTerm.occurrenceID.qualifiedName(), idCore);
    coreMap.put(somethingCore, somethingCore);

    // Ext
    Map<String, String> extCoreMap = new HashMap<>(2);
    extCoreMap.put(DwcTerm.occurrenceID.qualifiedName(), idExt);
    extCoreMap.put(somethingExt, somethingExt);

    Map<String, String> extMap = new HashMap<>(2);
    extMap.put(DwcTerm.occurrenceID.qualifiedName(), idExt);
    extMap.put(somethingExt, somethingExt);

    // Set
    Map<String, List<Map<String, String>>> exts = new HashMap<>(2);
    exts.put(Occurrence.qualifiedName(), Collections.singletonList(extCoreMap));
    exts.put(MeasurementOrFact.qualifiedName(), Arrays.asList(extMap, extMap));
    exts.put(ResourceRelationship.qualifiedName(), Arrays.asList(extMap, extMap));

    ExtendedRecord extendedRecord =
        ExtendedRecord.newBuilder().setId(idCore).setCoreTerms(coreMap).setExtensions(exts).build();

    // When
    List<ExtendedRecord> result = OccurrenceExtensionConverter.convert(extendedRecord);

    // Should
    Assert.assertFalse(result.isEmpty());

    ExtendedRecord erResult = result.get(0);

    Assert.assertEquals(idExt, erResult.getId());
    Assert.assertEquals(somethingCore, erResult.getCoreTerms().get(somethingCore));
    Assert.assertEquals(somethingExt, erResult.getCoreTerms().get(somethingExt));

    Assert.assertEquals(2, erResult.getExtensions().get(MeasurementOrFact.qualifiedName()).size());
    Assert.assertEquals(
        2, erResult.getExtensions().get(MeasurementOrFact.qualifiedName()).get(0).size());

    Assert.assertEquals(
        2, erResult.getExtensions().get(ResourceRelationship.qualifiedName()).size());
    Assert.assertEquals(
        2, erResult.getExtensions().get(ResourceRelationship.qualifiedName()).get(0).size());

    // coreId has the id reported in the Core
    Assert.assertEquals(idCore, erResult.getCoreId());
  }
}
