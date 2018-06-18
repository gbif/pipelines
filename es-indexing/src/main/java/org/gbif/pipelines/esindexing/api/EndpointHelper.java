package org.gbif.pipelines.esindexing.api;

/**
 * Helper to handle the ES endpoints.
 */
public class EndpointHelper {

  private static final String ROOT = "/";
  private static final String ALIASES_ENDPOINT = "/_aliases";

  private EndpointHelper() {}

  /**
   * Returns the aliases API endpoint (/_aliases).
   */
  public static String getAliasesEndpoint() {
    return ALIASES_ENDPOINT;
  }

  /**
   * Returns the endpoint to retrieve the indexes in an alias (/{idx}/_alias/{alias}).
   */
  public static String getAliasIndexexEndpoint(String idxPattern, String alias) {
    return ROOT + idxPattern + "/_alias/" + alias;
  }

  /**
   * Return the index endpoint for an index (/{index}).
   */
  public static String getIndexEndpoint(String index) {
    return ROOT + index;
  }

  /**
   * Returns the index settings endpoint for an index (/{idx}/_settings).
   */
  public static String getIndexSettingsEndpoint(String index) {
    return ROOT + index + "/_settings";
  }

  /**
   * Returns the index mappings endpoint (/{idx}/_mapping).
   */
  public static String getIndexMappingsEndpoint(String index) { return ROOT + index + "/_mapping"; }

  /**
   * Returns the index count enpoint (/{idx}/_count).
   */
  public static String getIndexCountEndpoint(String index) {
    return ROOT + index + "/_count/";
  }

  /**
   * Returns the index refresh endpoint (/{index}/_refresh).
   */
  public static String getRefreshIndexEndpoint(String index) {
    return EndpointHelper.getIndexEndpoint(index) + "/_refresh";
  }

}
