package org.gbif.pipelines.keygen.config;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * Configs needed to connect to the occurrence HBase db.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor(staticName = "create")
public class OccHbaseConfiguration implements Serializable {

  private static final long serialVersionUID = -4728303272918599949L;

  @NonNull
  private String occTable;

  @NonNull
  private String counterTable;

  @NonNull
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
}
