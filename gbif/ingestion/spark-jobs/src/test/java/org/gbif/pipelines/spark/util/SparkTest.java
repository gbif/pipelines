package org.gbif.pipelines.spark.util;

import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Static test utilities for Spark-based tests.
 *
 * <p>Provides helpers shared across test classes without imposing any inheritance or lifecycle
 * coupling. Each test class manages its own {@link SparkSession} lifecycle explicitly via
 * {@code @BeforeAll}/{@code @AfterAll}.
 *
 * <ul>
 *   <li>{@link #schema(String...)} — builds an all-string {@link StructType} from column names.
 *   <li>{@link #writeParquet(SparkSession, Path, String, StructType, List)} — writes rows to a
 *       Parquet file under a temp directory.
 * </ul>
 */
public final class SparkTest {

  private SparkTest() {}

  /**
   * Builds an all-nullable, all-string {@link StructType} from the given column names.
   *
   * <p>Sufficient for the string-typed DwC term columns in DwC-DP test fixtures. Use a manually
   * constructed schema when non-string types are needed.
   */
  public static StructType schema(String... names) {
    StructField[] fields = new StructField[names.length];
    for (int i = 0; i < names.length; i++) {
      fields[i] = DataTypes.createStructField(names[i], DataTypes.StringType, true);
    }
    return DataTypes.createStructType(fields);
  }

  /**
   * Writes {@code rows} to a Parquet file at {@code dir/relativePath}.
   *
   * <p>The path is prefixed with {@code file://} so Spark's local filesystem reader handles it
   * correctly regardless of whether HDFS is configured.
   */
  public static void writeParquet(
      SparkSession spark, Path dir, String relativePath, StructType schema, List<Row> rows) {
    spark.createDataFrame(rows, schema).write().parquet("file://" + dir.resolve(relativePath));
  }
}
