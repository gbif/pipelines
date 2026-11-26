package org.gbif.pipelines.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.io.avro.grscicoll.GrscicollRecord;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.gbif.pipelines.spark.pojo.legacy.LegacyOccurrence;

/**
 * Loads {@code simple_occurrence} Parquet as typed {@link Occurrence} records.
 *
 * <p>Attempts written before the typed schema stored each transform output as a JSON string. The
 * refresh pipelines and regeneration jobs read earlier attempts, so this loader detects that format
 * and converts it on read. Remove the legacy path once no retained attempt uses it.
 */
@Slf4j
public class SimpleRecordLoader {

  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  private SimpleRecordLoader() {}

  /**
   * @return true if the schema is the legacy layout, with JSON strings instead of nested structs
   */
  public static boolean isLegacyJson(StructType schema) {
    return schema.getFieldIndex("identifier").isDefined()
        && schema.apply("identifier").dataType() instanceof StringType;
  }

  /** Reads simple occurrences from the path, converting the legacy JSON format if needed. */
  public static Dataset<Occurrence> loadOccurrences(SparkSession spark, String path) {
    return toOccurrences(spark.read().parquet(path), path);
  }

  /** Converts a DataFrame read from simple_occurrence Parquet, in either format. */
  public static Dataset<Occurrence> toOccurrences(Dataset<Row> df, String pathForLogging) {
    if (isLegacyJson(df.schema())) {
      log.warn("{} uses the legacy JSON-string layout, converting on read", pathForLogging);
      return df.as(Encoders.bean(LegacyOccurrence.class))
          .map(
              (MapFunction<LegacyOccurrence, Occurrence>) SimpleRecordLoader::fromLegacy,
              Encoders.bean(Occurrence.class));
    }
    return df.as(Encoders.bean(Occurrence.class));
  }

  /** Converts one legacy record, parsing each JSON column into its typed record. */
  public static Occurrence fromLegacy(LegacyOccurrence l) throws IOException {
    return Occurrence.builder()
        .id(l.getId())
        .coreId(l.getCoreId())
        .internalId(l.getInternalId())
        .verbatim(read(l.getVerbatim(), ExtendedRecord.class))
        .identifier(read(l.getIdentifier(), IdentifierRecord.class))
        .basic(read(l.getBasic(), BasicRecord.class))
        .location(read(l.getLocation(), LocationRecord.class))
        .taxon(read(l.getTaxon(), MultiTaxonRecord.class))
        .temporal(read(l.getTemporal(), TemporalRecord.class))
        .grscicoll(read(l.getGrscicoll(), GrscicollRecord.class))
        .dnaDerivedData(read(l.getDnaDerivedData(), DnaDerivedDataRecord.class))
        .multimedia(read(l.getMultimedia(), MultimediaRecord.class))
        .audubon(read(l.getAudubon(), AudubonRecord.class))
        .image(read(l.getImage(), ImageRecord.class))
        .clustering(read(l.getClustering(), ClusteringRecord.class))
        .build();
  }

  private static <T> T read(String json, Class<T> clazz) throws IOException {
    return json == null || json.isEmpty() ? null : MAPPER.readValue(json, clazz);
  }
}
