package org.gbif.pipelines.spark.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.spark.dwcdp.DwcDpVerbatimConverter;
import org.gbif.pipelines.spark.dwcdp.model.DataPackage;

/**
 * Test-only factory for {@link TableLoader} instances.
 *
 * <p>Builds a loader that resolves table names to Parquet paths via a {@link DataPackage}
 * descriptor, exactly as the production {@link DwcDpVerbatimConverter} does. Centralised here so
 * that both unit and integration tests use a single, consistent wiring rather than repeating the
 * lambda inline.
 */
public final class TestTableLoader {

  private TestTableLoader() {}

  /**
   * Returns a {@link TableLoader} backed by {@code spark.read().parquet(basePath + "/" +
   * resource.getPath())} for each table present in {@code dataPackage}. Returns {@link
   * Optional#empty()} for tables not listed in the descriptor, matching the production loader's
   * behaviour.
   */
  public static TableLoader parquetLoader(
      SparkSession spark, DataPackage dataPackage, String basePath) {
    return tableName ->
        dataPackage
            .findResource(tableName)
            .map(r -> spark.read().parquet(basePath + "/" + r.getPath()));
  }

  /**
   * Builds a {@link TableLoader} from alternating name/dataset pairs.
   *
   * @param pairs alternating {@code String} table name and {@code Dataset<Row>} pairs
   * @throws IllegalArgumentException if an odd number of arguments is provided or a name is not a
   *     String or a dataset is not a Dataset
   */
  @SuppressWarnings("unchecked")
  public static TableLoader of(Object... pairs) {
    if (pairs.length % 2 != 0) {
      throw new IllegalArgumentException(
          "TestTableLoader.of() requires alternating name/dataset pairs, got "
              + pairs.length
              + " arguments");
    }

    Map<String, Dataset<Row>> tables = new HashMap<>();
    for (int i = 0; i < pairs.length; i += 2) {
      if (!(pairs[i] instanceof String)) {
        throw new IllegalArgumentException(
            "Argument at index "
                + i
                + " must be a String table name, got: "
                + pairs[i].getClass().getSimpleName());
      }
      if (!(pairs[i + 1] instanceof Dataset)) {
        throw new IllegalArgumentException(
            "Argument at index "
                + (i + 1)
                + " must be a Dataset<Row>, got: "
                + pairs[i + 1].getClass().getSimpleName());
      }
      tables.put((String) pairs[i], (Dataset<Row>) pairs[i + 1]);
    }

    return tableName -> Optional.ofNullable(tables.get(tableName));
  }

  /**
   * Builds a {@link TableLoader} from a pre-constructed map.
   *
   * <p>Use when the table map is built programmatically rather than inline.
   */
  public static TableLoader of(Map<String, Dataset<Row>> tables) {
    return tableName -> Optional.ofNullable(tables.get(tableName));
  }
}
