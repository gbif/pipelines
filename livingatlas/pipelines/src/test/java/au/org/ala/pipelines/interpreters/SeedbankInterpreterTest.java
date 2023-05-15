package au.org.ala.pipelines.interpreters;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;

import java.util.Collections;
import org.gbif.pipelines.io.avro.SeedbankRecord;
import org.junit.Before;
import org.junit.Test;

public class SeedbankInterpreterTest {
  private SeedbankInterpreter interpreter;

  private SeedbankRecord sr = SeedbankRecord.newBuilder().build();

  @Before
  public void setUp() {
    interpreter = SeedbankInterpreter.builder().orderings(Collections.emptyList()).create();
  }

  @Test
  public void testSetSampleWeight() {
    SeedbankInterpreter.setQuantityInGrams(sr, "10.0");
    assertEquals(10.0, sr.getQuantityInGrams());
  }

  @Test
  public void testSetSampleSize() {
    SeedbankInterpreter.setQuantityCount(sr, "100.0");
    assertEquals(100.0, sr.getQuantityCount());
  }

  @Test
  public void testSetAdjustedGermination() {
    SeedbankInterpreter.setAdjustedGermination(sr, "75");
    assertEquals(75.0, sr.getAdjustedGerminationPercentage());
  }

  @Test
  public void testSetDarkHours() {
    SeedbankInterpreter.setDarkHours(sr, "12.5");
    assertEquals(12.5, sr.getDarkHours());
  }

  @Test
  public void testSetDayTemp() {
    SeedbankInterpreter.setDayTemp(sr, "22.5");
    assertEquals(22.5, sr.getDayTemperatureInCelsius());
  }

  @Test
  public void testSetLightHours() {
    SeedbankInterpreter.setLightHours(sr, "11.5");
    assertEquals(11.5, sr.getLightHours());
  }

  @Test
  public void testSetNightTemperatureInCelsius() {
    SeedbankInterpreter.setNightTemperatureInCelsius(sr, "18.0");
    assertEquals(18.0, sr.getNightTemperatureInCelsius());
  }

  @Test
  public void testSetSampleSizeBadInput() {
    SeedbankInterpreter.setQuantityCount(sr, "invalid_input");
    assertNull(sr.getQuantityCount());
  }

  @Test
  public void testSetAdjustedGerminationBadInput() {
    SeedbankInterpreter.setAdjustedGermination(sr, "invalid_input");
    assertNull(sr.getAdjustedGerminationPercentage());
  }

  @Test
  public void testSetDarkHoursBadInput() {
    SeedbankInterpreter.setDarkHours(sr, "invalid_input");
    assertNull(sr.getDarkHours());
  }

  @Test
  public void testSetDayTempBadInput() {
    SeedbankInterpreter.setDayTemp(sr, "invalid_input");
    assertNull(sr.getDayTemperatureInCelsius());
  }

  @Test
  public void testSetLightHoursBadInput() {
    SeedbankInterpreter.setLightHours(sr, "invalid_input");
    assertNull(sr.getLightHours());
  }

  @Test
  public void testSetNightTemperatureInCelsiusBadInput() {
    SeedbankInterpreter.setNightTemperatureInCelsius(sr, "invalid_input");
    assertNull(sr.getNightTemperatureInCelsius());
  }
}
