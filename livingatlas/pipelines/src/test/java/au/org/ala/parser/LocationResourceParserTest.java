package au.org.ala.parser;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import au.org.ala.pipelines.vocabulary.CentrePoints;
import au.org.ala.pipelines.vocabulary.CountryCentrePoints;
import au.org.ala.pipelines.vocabulary.StateProvinceCentrePoints;
import au.org.ala.pipelines.vocabulary.StateProvinceParser;
import java.io.FileNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocationResourceParserTest {

  private ALAPipelinesConfig alaConfig;

  @Before
  public void setup() {
    // use the files in /resources
    LocationInfoConfig liConfig = new LocationInfoConfig();
    alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(liConfig);
  }

  @Test
  public void countryCentreTest() throws FileNotFoundException {
    boolean result =
        CountryCentrePoints.getInstance(alaConfig.getLocationInfoConfig())
            .coordinatesMatchCentre("AUSTRALIA", -25.733, 134.491);
    Assert.assertFalse(result);

    boolean result1 =
        CountryCentrePoints.getInstance(alaConfig.getLocationInfoConfig())
            .coordinatesMatchCentre("AUSTRALIA", -29.53, 145.4915);
    Assert.assertTrue(result1);
  }

  @Test
  public void stateNameMatchingTest() throws FileNotFoundException {
    Assert.assertEquals(
        "Queensland",
        StateProvinceParser.getInstance(
                alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .parse("QLD")
            .getPayload());
    Assert.assertEquals(
        "Victoria",
        StateProvinceParser.getInstance(
                alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .parse("VIC")
            .getPayload());
  }

  @Test
  public void stateCentreMatchingTest() throws FileNotFoundException {
    CentrePoints centrePoints =
        StateProvinceCentrePoints.getInstance(alaConfig.getLocationInfoConfig());
    Assert.assertFalse(centrePoints.coordinatesMatchCentre("UNSW", 10.1, 10.1));
    Assert.assertTrue(
        centrePoints.coordinatesMatchCentre("Northern Territory", -19.4914108, 132.5509603));
    Assert.assertTrue(
        centrePoints.coordinatesMatchCentre("Western Australia", -27.672817, 121.62831));
    Assert.assertTrue(centrePoints.coordinatesMatchCentre("Western Australia", -27.7, 121.6));
  }
}
