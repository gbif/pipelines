package org.gbif.pipelines.spark.dwcdp.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.OccurrenceExtensionBuilder;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds a distributed Dataset of event-core {@link ExtendedRecord}s from DwC-DP Parquet files.
 *
 * <p>Pipeline:
 *
 * <ol>
 *   <li>Load the required {@code event} table — throws if absent (routing error).
 *   <li>Build the Occurrence extension via {@link OccurrenceExtensionBuilder} — skipped if the
 *       occurrence table is absent or has no {@code eventID} column. Organism fields are already
 *       denormalized onto occurrence rows inside that builder.
 *   <li>Build the Multimedia extension via {@link MediaExtensionBuilder} — skipped if either {@code
 *       event-media} or {@code media} is absent.
 *   <li>Map each joined row to an {@link ExtendedRecord} with {@code coreRowType = dwc:Event}.
 * </ol>
 */
@Slf4j
public class EventCoreBuilder {

  private static final String CORE_ROW_TYPE = DwcTerm.Event.qualifiedName();
  private static final String ROW_TYPE_OCCURRENCE = DwcTerm.Occurrence.qualifiedName();
  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  private EventCoreBuilder() {}

  /**
   * Builds the event-core ExtendedRecord Dataset.
   *
   * @param spark active SparkSession
   * @param loader table loader — {@link Optional#empty()} signals a table is absent from the
   *     package
   * @throws IllegalStateException if the event table is absent (caller routing error)
   */
  public static Dataset<ExtendedRecord> build(SparkSession spark, TableLoader loader) {

    Dataset<Row> eventDf =
        loader
            .load("event")
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "event table missing — orchestrator should not have routed here"));

    Optional<Dataset<Row>> occurrenceExtDf = OccurrenceExtensionBuilder.build(spark, loader);
    Optional<Dataset<Row>> mediaExtDf =
        MediaExtensionBuilder.buildEventMediaExtension(spark, loader);

    Dataset<Row> joined = eventDf;
    if (occurrenceExtDf.isPresent()) {
      joined =
          joined
              .join(
                  occurrenceExtDf.get(),
                  joined.col("eventID").equalTo(occurrenceExtDf.get().col("eventID")),
                  "left_outer")
              .drop(occurrenceExtDf.get().col("eventID"));
    }
    if (mediaExtDf.isPresent()) {
      joined =
          joined
              .join(
                  mediaExtDf.get(),
                  joined.col("eventID").equalTo(mediaExtDf.get().col("eventID")),
                  "left_outer")
              .drop(mediaExtDf.get().col("eventID"));
    }

    final String[] eventColumns = eventDf.columns();
    final boolean hasOccExt = occurrenceExtDf.isPresent();
    final boolean hasMediaExt = mediaExtDf.isPresent();

    return joined
        .map(
            (MapFunction<Row, ExtendedRecord>)
                row -> toExtendedRecord(row, eventColumns, hasOccExt, hasMediaExt),
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) r -> r != null);
  }

  private static ExtendedRecord toExtendedRecord(
      Row row, String[] eventColumns, boolean hasOccExt, boolean hasMediaExt) throws IOException {

    String eventId = RowTermMapper.safeGet(row, "eventID");
    if (eventId == null || eventId.isEmpty()) {
      return null;
    }

    Map<String, String> coreTerms = RowTermMapper.toTermMap(row, eventColumns);
    Map<String, List<Map<String, String>>> extensions = new HashMap<>();

    if (hasOccExt) {
      String occJson =
          RowTermMapper.safeGet(row, OccurrenceExtensionBuilder.COL_OCCURRENCE_EXT_JSON);
      if (occJson != null) {
        extensions.put(ROW_TYPE_OCCURRENCE, fromJson(occJson));
      }
    }
    if (hasMediaExt) {
      String mediaJson = RowTermMapper.safeGet(row, MediaExtensionBuilder.COL_MEDIA_EXT_JSON);
      if (mediaJson != null) {
        extensions.put(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA, fromJson(mediaJson));
      }
    }

    return ExtendedRecord.newBuilder()
        .setId(eventId)
        .setCoreId(null)
        .setCoreRowType(CORE_ROW_TYPE)
        .setCoreTerms(coreTerms)
        .setExtensions(extensions)
        .build();
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, String>> fromJson(String json) throws IOException {
    return MAPPER.readValue(json, List.class);
  }
}
