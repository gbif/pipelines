package au.org.ala.parser;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import au.org.ala.pipelines.vocabulary.StateProvinceParser;
import java.io.File;
import java.io.FileNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocationExtResourceParserTest {

  private ALAPipelinesConfig alaConfig;

  @Before
  public void setup() {
    String absolutePath = new File("src/test/resources").getAbsolutePath();
    LocationInfoConfig extLiConfig =
        new LocationInfoConfig(null, null, absolutePath + "/stateProvincesTest.tsv");
    alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(extLiConfig);
  }

  @Test
  public void stateNameMatchingTest() throws FileNotFoundException {
    Assert.assertEquals(
        "New Yorkistan",
        StateProvinceParser.getInstance(
                alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .parse("New Yorkistan")
            .getPayload());
  }
}
