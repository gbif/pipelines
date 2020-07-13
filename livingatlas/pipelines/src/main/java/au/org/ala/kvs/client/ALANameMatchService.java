package au.org.ala.kvs.client;

import java.io.Closeable;

public interface ALANameMatchService extends Closeable {

  /**
   * Matches scientific names against the ALA Taxonomy.
   *
   * @return a possible null name match
   */
  ALANameUsageMatch match(ALASpeciesMatchRequest key);
}
