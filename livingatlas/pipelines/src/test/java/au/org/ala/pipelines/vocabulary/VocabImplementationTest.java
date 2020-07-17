package au.org.ala.pipelines.vocabulary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import org.junit.Test;

public class VocabImplementationTest {

  @Test
  public void testStateProvinceWithClasspathResource() throws IOException {
    ALAPipelinesConfig alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(new LocationInfoConfig());

    assertEquals(
        Optional.of("Australian Capital Territory"),
        StateProvince.getInstance(alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .matchTerm("ACT"));

    // nterritory
    assertEquals(
        Optional.of("Tasmania"),
        StateProvince.getInstance(alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .matchTerm("tasmania"));
  }

  /** Missing external resources files   */
  @Test
  public void testNoneExistsExternalResource() throws IOException {
    ALAPipelinesConfig alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(
        new LocationInfoConfig("none_exists.txt", "none_exists.txt", "none_exists.txt"));

    Exception stateNameException =
        assertThrows(
            FileNotFoundException.class,
            () -> {
              StateProvince.getInstance(
                      alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
                  .matchTerm("ACT");
            });
    assertTrue(stateNameException.getMessage().contains("none_exists.txt"));

    Exception stateCentreException =
        assertThrows(
            FileNotFoundException.class,
            () -> {
              StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig())
                  .coordinatesMatchCentre("Test", 0, 0);
            });
    assertTrue(stateCentreException.getMessage().contains("none_exists.txt"));

    Exception countryCentreException =
        assertThrows(
            FileNotFoundException.class,
            () -> {
              CountryCentrePoints.getInstance(alaConfig.getLocationInfoConfig())
                  .coordinatesMatchCentre("Test", 0, 0);
            });
    assertTrue(countryCentreException.getMessage().contains("none_exists.txt"));
  }
}
