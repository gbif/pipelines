package au.org.ala.pipelines.transforms;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.gbif.common.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.gbif.common.shaded.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.junit.jupiter.api.Test;

class IndexRecordTransformTest {

  private IndexRecord.Builder newBuilderWithStrings() {
    IndexRecord.Builder b = IndexRecord.newBuilder().setId("someId");
    b.setStrings(new HashMap<>());
    return b;
  }

  @Test
  void parseDynamicProperties_givenNullValue_thenEmptyProperties() {
    String input = null;
    IndexRecord.Builder b = newBuilderWithStrings();

    IndexRecordTransform.parseDynamicProperties(input, b);

    // dynamicProperties should not be set, but the strings map should contain the key with null
    assertTrue(b.getStrings().containsKey(DwcTerm.dynamicProperties.simpleName()));
    assertNull(b.getStrings().get(DwcTerm.dynamicProperties.simpleName()));
    assertFalse(b.hasDynamicProperties());
  }

  @Test
  void parseDynamicProperties_givenEmptyString_thenEmptyProperties() {
    String input = "";
    IndexRecord.Builder b = newBuilderWithStrings();

    IndexRecordTransform.parseDynamicProperties(input, b);

    assertTrue(b.getStrings().containsKey(DwcTerm.dynamicProperties.simpleName()));
    assertEquals("", b.getStrings().get(DwcTerm.dynamicProperties.simpleName()));
    assertFalse(b.hasDynamicProperties());
  }

  @Test
  void parseDynamicProperties_givenValidJson_thenPropertiesParsedCorrectly() {
    IndexRecord.Builder b = newBuilderWithStrings();
    String json = "{\"a\":\"1\",\"b\":2,\"c\":null}";

    IndexRecordTransform.parseDynamicProperties(json, b);

    // original string preserved
    assertEquals(json, b.getStrings().get(DwcTerm.dynamicProperties.simpleName()));
    // dynamicProperties parsed and values converted to strings (null -> "")
    assertTrue(b.hasDynamicProperties());
    Map<String, String> dp = b.getDynamicProperties();
    assertEquals("1", dp.get("a"));
    assertEquals("2", dp.get("b"));
    assertEquals("", dp.get("c"));
  }

  @Test
  void parseDynamicProperties_givenCsvEscapedJson_thenPropertiesParsedCorrectly()
      throws JsonProcessingException {
    IndexRecord.Builder b = newBuilderWithStrings();
    CsvMapper mapper = new CsvMapper();
    String inputJson = "{\"a\":\"1\",\"b\":2}";
    String csvEscaped = mapper.writeValueAsString(inputJson).trim();

    IndexRecordTransform.parseDynamicProperties(csvEscaped, b);

    assertEquals(csvEscaped, b.getStrings().get(DwcTerm.dynamicProperties.simpleName()));
    assertTrue(b.hasDynamicProperties());
    Map<String, String> dp = b.getDynamicProperties();
    assertEquals("1", dp.get("a"));
    assertEquals("2", dp.get("b"));
  }

  @Test
  void parseDynamicProperties_givenInvalidJson_thenEmptyProperties() {
    IndexRecord.Builder b = newBuilderWithStrings();
    String invalid = "not a json";

    IndexRecordTransform.parseDynamicProperties(invalid, b);

    // original string preserved, but parsing fails so dynamicProperties not set
    assertEquals(invalid, b.getStrings().get(DwcTerm.dynamicProperties.simpleName()));
    assertFalse(b.hasDynamicProperties());
  }

  @Test
  void parseDynamicProperties_givenNonStringValues_thenPropertiesWithStringRepresentations() {
    IndexRecord.Builder b = newBuilderWithStrings();
    String json = "{\"arr\":[1,2],\"obj\":{\"x\":1},\"flag\":true}";

    IndexRecordTransform.parseDynamicProperties(json, b);

    assertTrue(b.hasDynamicProperties());
    Map<String, String> dp = b.getDynamicProperties();
    assertEquals("[1, 2]", dp.get("arr") == null ? null : dp.get("arr"));
    assertNotNull(dp.get("obj"));
    assertEquals("true", dp.get("flag"));
  }
}
