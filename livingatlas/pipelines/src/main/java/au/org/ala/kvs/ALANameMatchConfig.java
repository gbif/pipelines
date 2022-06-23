package au.org.ala.kvs;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class ALANameMatchConfig implements Serializable {

  private Boolean matchOnTaxonID = true;
}
