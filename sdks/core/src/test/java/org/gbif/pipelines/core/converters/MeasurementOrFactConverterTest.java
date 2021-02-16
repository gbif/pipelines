package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class MeasurementOrFactConverterTest {

  private static final String ID = "777";

  @Test
  public void converterTest() {

    // Expect

    // State
    String value = "sex=female;age class=adult;total length=495 mm;tail length=210 mm;weight=100g";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(ID)
            .setCoreTerms(
                Collections.singletonMap(DwcTerm.dynamicProperties.qualifiedName(), value))
            .build();

    // When
    List<Map<String, String>> result = MeasurementOrFactConverter.extractFromDynamicProperties(er);

    // Should
    Assert.assertEquals(2, result.size());

    Assert.assertTrue(hasValue(result, DwcTerm.measurementType, "total weight"));
    Assert.assertTrue(hasValue(result, DwcTerm.measurementValue, "100"));
    Assert.assertTrue(hasValue(result, DwcTerm.measurementUnit, "g"));

    Assert.assertTrue(hasValue(result, DwcTerm.measurementType, "total length"));
    Assert.assertTrue(hasValue(result, DwcTerm.measurementValue, "495"));
    Assert.assertTrue(hasValue(result, DwcTerm.measurementUnit, "mm"));
  }

  @Test
  public void emptyTest() {

    // State
    String value = "sex=female;age class=adult";
    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(ID)
            .setCoreTerms(
                Collections.singletonMap(DwcTerm.dynamicProperties.qualifiedName(), value))
            .build();

    // When
    List<Map<String, String>> result = MeasurementOrFactConverter.extractFromDynamicProperties(er);

    // Should
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void emptyDynamicPropertiesTest() {

    // State
    ExtendedRecord er =
        ExtendedRecord.newBuilder().setId(ID).setCoreTerms(Collections.emptyMap()).build();

    // When
    List<Map<String, String>> result = MeasurementOrFactConverter.extractFromDynamicProperties(er);

    // Should
    Assert.assertTrue(result.isEmpty());
  }

  private static boolean hasValue(List<Map<String, String>> list, DwcTerm term, String value) {
    return list.stream()
        .filter(x -> x.get(term.qualifiedName()).equals(value))
        .map(x -> x.get(term.qualifiedName()))
        .findAny()
        .isPresent();
  }
}
