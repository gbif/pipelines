package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.gbif.pipelines.spark.dwcdp.builder.RowTermMapper;
import org.gbif.pipelines.spark.util.MapperUtil;

/**
 * Groups a Dataset by a key column and serialises all rows per key into a JSON array of term maps.
 *
 * <p>Used by all extension builders ({@link MediaExtensionBuilder}, {@link
 * OccurrenceExtensionBuilder}) to produce intermediate two-column Datasets that the core builders
 * later left-join and attach as named extensions on {@link
 * org.gbif.pipelines.io.avro.ExtendedRecord}.
 */
final class ExtensionAggregator {

  private ExtensionAggregator() {}

  /**
   * Groups {@code df} by {@code keyColumn}, serialises each group's rows into a JSON array of
   * {@link RowTermMapper#toTermMap term maps}, and returns a two-column Dataset: {@code (keyColumn,
   * jsonColumnName)}.
   */
  static Dataset<Row> aggregateAsJsonByKey(
      SparkSession spark,
      Dataset<Row> df,
      String[] allColumns,
      String keyColumn,
      String jsonColumnName) {

    return df.groupByKey(
            (MapFunction<Row, String>) row -> RowTermMapper.safeGet(row, keyColumn),
            Encoders.STRING())
        .mapGroups(
            (MapGroupsFunction<String, Row, Row>)
                (key, iter) -> {
                  List<Map<String, String>> termMaps = new ArrayList<>();
                  while (iter.hasNext()) {
                    termMaps.add(RowTermMapper.toTermMap(iter.next(), allColumns));
                  }
                  return RowFactory.create(key, MapperUtil.MAPPER.writeValueAsString(termMaps));
                },
            Encoders.row(
                DataTypes.createStructType(
                    new StructField[] {
                      DataTypes.createStructField(keyColumn, DataTypes.StringType, true),
                      DataTypes.createStructField(jsonColumnName, DataTypes.StringType, true)
                    })));
  }
}
