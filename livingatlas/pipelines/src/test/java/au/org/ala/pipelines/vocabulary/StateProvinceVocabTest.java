package au.org.ala.pipelines.vocabulary;

import static org.junit.Assert.assertEquals;

import au.org.ala.kvs.ALAPipelinesConfig;
import au.org.ala.kvs.LocationInfoConfig;
import java.io.IOException;
import java.util.Optional;
import org.junit.Test;

public class StateProvinceVocabTest {

  @Test
  public void testStateProvince() throws IOException {
    ALAPipelinesConfig alaConfig = new ALAPipelinesConfig();
    alaConfig.setLocationInfoConfig(new LocationInfoConfig(null, null, null));
    assertEquals(
        Optional.of("Australian Capital Territory"),
        StateProvince.getInstance(alaConfig.getLocationInfoConfig().getStateProvinceNamesFile())
            .matchTerm("ACT"));
  }
}
