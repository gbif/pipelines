package au.org.ala.pipelines.vocabulary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.Test;

public class StateProvinceVocabTest {

  @Test
  public void testStateProvinceWithClasspathResource() throws IOException {
    ALAPipelinesConfig alaConfig = new ALAPipelinesConfig();
    LocationInfoConfig locationInfoConfig = new LocationInfoConfig();
    alaConfig.setLocationInfoConfig(locationInfoConfig);

    assertEquals(
        "Australian Capital Territory",
        StateProvinceParser.getInstance(
                alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .parse("ACT")
            .getPayload());

    // nterritory
    assertEquals(
        "Tasmania",
        StateProvinceParser.getInstance(
                alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .parse("tasmania")
            .getPayload());
  }

  /** Missing external resources files */
  @Test
  public void testNoneExistsExternalResource() {
    ALAPipelinesConfig alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(
        new LocationInfoConfig("none_exists.txt", "none_exists.txt", "none_exists.txt"));

    Exception stateNameException =
        assertThrows(
            FileNotFoundException.class,
            () ->
                StateProvinceParser.getInstance(
                        alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
                    .parse("ACT")
                    .getPayload());
    assertTrue(stateNameException.getMessage().contains("none_exists.txt"));

    Exception stateCentreException =
        assertThrows(
            FileNotFoundException.class,
            () ->
                StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig())
                    .coordinatesMatchCentre("Test", 0, 0));
    assertTrue(stateCentreException.getMessage().contains("none_exists.txt"));

    Exception countryCentreException =
        assertThrows(
            FileNotFoundException.class,
            () ->
                CountryCentrePoints.getInstance(alaConfig.getLocationInfoConfig())
                    .coordinatesMatchCentre("Test", 0, 0));
    assertTrue(countryCentreException.getMessage().contains("none_exists.txt"));
  }
}
