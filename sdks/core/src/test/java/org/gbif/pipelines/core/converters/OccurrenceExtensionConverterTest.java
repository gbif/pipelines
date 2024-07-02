package org.gbif.pipelines.core.converters;

import static org.gbif.dwc.terms.DwcTerm.MeasurementOrFact;
import static org.gbif.dwc.terms.DwcTerm.Occurrence;
import static org.gbif.dwc.terms.DwcTerm.ResourceRelationship;

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
    Map<String, String> coreMap =
        Map.of(DwcTerm.occurrenceID.qualifiedName(), idCore, somethingCore, somethingCore);

    // Ext
    Map<String, String> extCoreMap =
        Map.of(DwcTerm.occurrenceID.qualifiedName(), idExt, somethingExt, somethingExt);

    Map<String, String> extMap =
        Map.of(DwcTerm.occurrenceID.qualifiedName(), idExt, somethingExt, somethingExt);

    // Set
    Map<String, List<Map<String, String>>> exts =
        Map.of(
            Occurrence.qualifiedName(), List.of(extCoreMap),
            MeasurementOrFact.qualifiedName(), List.of(extMap, extMap),
            ResourceRelationship.qualifiedName(), List.of(extMap, extMap));

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
