package au.org.ala.kvs;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class LocationInfoConfig implements Serializable {
  private String countryCentrePointsFile;
  private String stateProvinceCentrePointsFile;
  private String stateProvinceNamesFile;
}
