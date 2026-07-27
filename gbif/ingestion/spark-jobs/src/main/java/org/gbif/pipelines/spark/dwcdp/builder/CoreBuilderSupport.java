package org.gbif.pipelines.spark.dwcdp.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.util.MapperUtil;

final class CoreBuilderSupport {

  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  private CoreBuilderSupport() {}

  static Dataset<Row> joinIfPresent(
      Dataset<Row> left, Optional<Dataset<Row>> rightOpt, String joinColumn) {
    return rightOpt
        .map(
            right ->
                left.join(right, left.col(joinColumn).equalTo(right.col(joinColumn)), "left_outer")
                    .drop(right.col(joinColumn)))
        .orElse(left);
  }

  static void addExtensionIfPresent(
      Row row,
      Map<String, List<Map<String, String>>> extensions,
      boolean hasExtension,
      String jsonColumn,
      String rowType)
      throws IOException {
    if (!hasExtension) {
      return;
    }

    String json = RowTermMapper.safeGet(row, jsonColumn);
    if (json != null) {
      extensions.put(rowType, fromJson(json));
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, String>> fromJson(String json) throws IOException {
    return MAPPER.readValue(json, List.class);
  }
}
