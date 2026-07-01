package org.gbif.pipelines.spark.util;

import java.util.Optional;
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
public final class TableLoaders {

  private TableLoaders() {}

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
}
