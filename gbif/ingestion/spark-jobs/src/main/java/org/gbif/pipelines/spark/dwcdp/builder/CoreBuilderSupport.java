package org.gbif.pipelines.spark.dwcdp.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.util.MapperUtil;

/**
 * Support for turning an extension builder's aggregated JSON column into the {@code
 * Map<rowType, List<Map<term, value>>>} shape {@link org.gbif.pipelines.io.avro.ExtendedRecord}
 * expects.
 */
final class CoreBuilderSupport {

  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  private CoreBuilderSupport() {}

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
