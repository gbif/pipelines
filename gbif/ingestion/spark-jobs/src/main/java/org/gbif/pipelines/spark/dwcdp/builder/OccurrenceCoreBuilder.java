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
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.OrganismJoinBuilder;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds a distributed Dataset of occurrence-core {@link ExtendedRecord}s from DwC-DP Parquet
 * files.
 *
 * <p>Pipeline:
 *
 * <ol>
 *   <li>Load the required {@code occurrence} table — throws if absent (routing error).
 *   <li>Left-join {@code organism} via {@link OrganismJoinBuilder} — skipped if absent.
 *   <li>Left-join {@code occurrence-media} + {@code media} via {@link MediaExtensionBuilder} and
 *       attach as Multimedia extension — skipped if either table is absent.
 *   <li>Build the eMoF extension via {@link AssertionExtensionBuilder} — skipped if {@code
 *       occurrence-assertion} is absent.
 *   <li>Map each enriched row to an {@link ExtendedRecord} with {@code coreRowType =
 *       dwc:Occurrence}.
 * </ol>
 */
@Slf4j
public class OccurrenceCoreBuilder {

  private static final String CORE_ROW_TYPE = DwcTerm.Occurrence.qualifiedName();
  private static final ObjectMapper MAPPER = MapperUtil.MAPPER;

  private OccurrenceCoreBuilder() {}

  /**
   * Builds the occurrence-core ExtendedRecord Dataset.
   *
   * @param spark active SparkSession
   * @param loader table loader — {@link Optional#empty()} signals a table is absent from the
   *     package
   * @throws IllegalStateException if the occurrence table is absent (caller routing error)
   */
  public static Dataset<ExtendedRecord> build(SparkSession spark, TableLoader loader) {

    Dataset<Row> occurrenceDf =
        loader
            .load("occurrence")
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "occurrence table missing — orchestrator should not have routed here"));

    Dataset<Row> enriched = OrganismJoinBuilder.enrichOccurrences(loader, occurrenceDf);

    Optional<Dataset<Row>> mediaExtDf =
        MediaExtensionBuilder.buildOccurrenceMediaExtension(spark, loader);
    Optional<Dataset<Row>> assertionExtDf =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(spark, loader);

    Dataset<Row> joined = enriched;
    if (mediaExtDf.isPresent()) {
      joined =
          joined
              .join(
                  mediaExtDf.get(),
                  enriched.col("occurrenceID").equalTo(mediaExtDf.get().col("occurrenceID")),
                  "left_outer")
              .drop(mediaExtDf.get().col("occurrenceID"));
    }
    if (assertionExtDf.isPresent()) {
      joined =
          joined
              .join(
                  assertionExtDf.get(),
                  joined.col("occurrenceID").equalTo(assertionExtDf.get().col("occurrenceID")),
                  "left_outer")
              .drop(assertionExtDf.get().col("occurrenceID"));
    }

    final String[] occColumns = enriched.columns();
    final boolean hasMediaExt = mediaExtDf.isPresent();
    final boolean hasAssertionExt = assertionExtDf.isPresent();

    return joined
        .map(
            (MapFunction<Row, ExtendedRecord>)
                row -> toExtendedRecord(row, occColumns, hasMediaExt, hasAssertionExt),
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) r -> r != null);
  }

  private static ExtendedRecord toExtendedRecord(
      Row row, String[] occColumns, boolean hasMediaExt, boolean hasAssertionExt)
      throws IOException {

    String occurrenceId = RowTermMapper.safeGet(row, "occurrenceID");
    if (occurrenceId == null || occurrenceId.isEmpty()) {
      return null;
    }

    Map<String, String> coreTerms = RowTermMapper.toTermMap(row, occColumns);
    Map<String, List<Map<String, String>>> extensions = new HashMap<>();

    if (hasMediaExt) {
      String mediaJson = RowTermMapper.safeGet(row, MediaExtensionBuilder.COL_MEDIA_EXT_JSON);
      if (mediaJson != null) {
        extensions.put(MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA, fromJson(mediaJson));
      }
    }
    if (hasAssertionExt) {
      String assertionJson =
          RowTermMapper.safeGet(row, AssertionExtensionBuilder.COL_ASSERTION_EXT_JSON);
      if (assertionJson != null) {
        extensions.put(
            AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT,
            fromJson(assertionJson));
      }
    }

    return ExtendedRecord.newBuilder()
        .setId(occurrenceId)
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
