package org.gbif.pipeleins.config;

import com.google.common.base.MoreObjects;
import javax.validation.constraints.NotNull;

/**
 * Configs needed to connect to the occurrence HBase db.
 */
public class OccHbaseConfiguration {

  @NotNull
  public String occTable;

  @NotNull
  public String counterTable;

  @NotNull
  public String lookupTable;


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
