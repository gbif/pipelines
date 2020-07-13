package au.org.ala.kvs;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Store configuration of SHP files including fields for ALA Country/State interpretation
 */
@AllArgsConstructor
@Data
public class GeocodeShpConfig implements Serializable {

  private ShapeFile country;
  private ShapeFile eez;
  private ShapeFile stateProvince;
}