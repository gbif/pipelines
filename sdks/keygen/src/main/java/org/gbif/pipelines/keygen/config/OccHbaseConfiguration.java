package org.gbif.pipelines.keygen.config;

import com.google.common.base.MoreObjects;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Configs needed to connect to the occurrence HBase db.
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class OccHbaseConfiguration {

  @NotNull
  private String occTable;

  @NotNull
  private String counterTable;

  @NotNull
  private String lookupTable;


  /**
   * Uses conventions to populate all table names based on the environment prefix. Only used in tests!
   *
   * @param prefix environment prefix, e.g. prod or uat
   */
  public void setEnvironment(String prefix) {
    occTable = prefix + "_occurrence";
    counterTable = prefix + "_occurrence_counter";
    lookupTable = prefix + "_occurrence_lookup";
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("occTable", occTable)
        .add("counterTable", counterTable)
        .add("lookupTable", lookupTable)
        .toString();
  }

}
