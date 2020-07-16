package au.org.ala.kvs;

import java.io.Serializable;
import javax.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@NoArgsConstructor
@AllArgsConstructor
@Data
public class LocationInfoConfig implements Serializable {
  private String countryCentrePointsFile;
  private String stateProvinceCentrePointsFile;
  private String stateProvinceNamesFile;
}
