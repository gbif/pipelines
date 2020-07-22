package au.org.ala.kvs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;
import org.gbif.pipelines.parsers.config.model.PipelinesConfig;
import org.gbif.pipelines.parsers.config.model.WsConfig;

/** Living Atlas configuration extensions */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ALAPipelinesConfig implements Serializable {

  PipelinesConfig gbifConfig;
  GeocodeShpConfig geocodeConfig;
  LocationInfoConfig locationInfoConfig;
  // ALA specific
  private WsConfig collectory;
  private WsConfig alaNameMatch;
  private WsConfig lists;

  public ALAPipelinesConfig() {
    gbifConfig = new PipelinesConfig();
    locationInfoConfig = new LocationInfoConfig();
    collectory = new WsConfig();
    alaNameMatch = new WsConfig();
    lists = new WsConfig();
  }
}
