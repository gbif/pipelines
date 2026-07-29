package org.gbif.pipelines.spark.dwcdp.builder;

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
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentificationExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentificationJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentifierExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialProtocolJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialProvenanceJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.OrganismJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.ProtocolJoinBuilder;
import org.gbif.pipelines.spark.util.DatasetJoins;
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
 *   <li>Left-join {@code identification} via {@link IdentificationJoinBuilder} — adds the taxonomic
 *       rank hierarchy occurrence never carries on its own; only applies when an occurrence has
 *       exactly one accepted identification, skipped otherwise.
 *   <li>Left-join {@code material} via {@link MaterialJoinBuilder} — adds institution/collection/
 *       specimen fields (institutionCode, catalogNumber, preparations, typeStatus, ..., plus
 *       license/rightsHolder from {@code usage-policy}); only applies when an occurrence has
 *       exactly one material row citing it as evidence, skipped otherwise.
 *   <li>Enrich with {@code fundingAttribution}/{@code fundingAttributionID}/{@code projectID}/
 *       {@code projectTitle} via {@link MaterialProvenanceJoinBuilder} — the occurrence's own
 *       material's provenance attribution, same exactly-one-material rule, same aggregation logic
 *       {@link org.gbif.pipelines.spark.dwcdp.builder.extension.ProvenanceJoinBuilder} applies for
 *       {@code event}.
 *   <li>Resolve {@code occurrenceProtocol_fk} → new {@code samplingProtocol} column via {@link
 *       ProtocolJoinBuilder} — previously leaked as a raw surrogate value under its own column
 *       name.
 *   <li>Drop the bare {@code occurrence_pk} surrogate — it's needed only so {@link
 *       MediaExtensionBuilder}/{@link AssertionExtensionBuilder} can resolve their own {@code
 *       occurrence_fk} against it (each reloads {@code occurrence} fresh from the loader for that,
 *       independently of this local Dataset), and has no DwC term of its own, so it must not
 *       survive into {@code coreTerms}.
 *   <li>Left-join {@code occurrence-media} + {@code media} via {@link MediaExtensionBuilder} and
 *       attach as Multimedia extension — skipped if either table is absent.
 *   <li>Build the eMoF extension via {@link AssertionExtensionBuilder} — skipped if {@code
 *       occurrence-assertion} is absent.
 *   <li>Build the Identification History extension via {@link IdentificationExtensionBuilder} —
 *       every {@code identification} row linked to the occurrence, accepted or not; distinct from
 *       the single-accepted flattening onto core terms two steps above.
 *   <li>Build the Identifier extension via {@link IdentifierExtensionBuilder} — merges {@code
 *       occurrence-identifier} with {@code material-identifier}, same exactly-one-material rule.
 *   <li>Map each enriched row to an {@link ExtendedRecord} with {@code coreRowType =
 *       dwc:Occurrence}.
 * </ol>
 */
@Slf4j
public class OccurrenceCoreBuilder {

  private static final String CORE_ROW_TYPE = DwcTerm.Occurrence.qualifiedName();
  private static final String OCCURRENCE_PK_COLUMN = "occurrence_pk";

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
    enriched = IdentificationJoinBuilder.enrichOccurrences(loader, enriched);
    enriched = MaterialJoinBuilder.enrichOccurrences(loader, enriched);
    enriched = MaterialProvenanceJoinBuilder.enrichOccurrences(loader, enriched);
    enriched =
        ProtocolJoinBuilder.resolveProtocolFk(
            loader, enriched, "occurrenceProtocol_fk", "samplingProtocol");
    enriched = MaterialProtocolJoinBuilder.enrichOccurrences(loader, enriched);
    enriched = enriched.drop(OCCURRENCE_PK_COLUMN);

    Optional<Dataset<Row>> mediaExtDf =
        MediaExtensionBuilder.buildOccurrenceMediaExtension(spark, loader);
    Optional<Dataset<Row>> assertionExtDf =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(spark, loader);
    Optional<Dataset<Row>> identificationExtDf =
        IdentificationExtensionBuilder.build(spark, loader);
    Optional<Dataset<Row>> identifierExtDf = IdentifierExtensionBuilder.build(spark, loader);

    Dataset<Row> joined = enriched;
    joined = DatasetJoins.leftJoinIfPresent(joined, mediaExtDf, "occurrenceID");
    joined = DatasetJoins.leftJoinIfPresent(joined, assertionExtDf, "occurrenceID");
    joined = DatasetJoins.leftJoinIfPresent(joined, identificationExtDf, "occurrenceID");
    joined = DatasetJoins.leftJoinIfPresent(joined, identifierExtDf, "occurrenceID");

    final String[] occColumns = enriched.columns();
    final boolean hasMediaExt = mediaExtDf.isPresent();
    final boolean hasAssertionExt = assertionExtDf.isPresent();
    final boolean hasIdentificationExt = identificationExtDf.isPresent();
    final boolean hasIdentifierExt = identifierExtDf.isPresent();

    return joined
        .map(
            (MapFunction<Row, ExtendedRecord>)
                row ->
                    toExtendedRecord(
                        row,
                        occColumns,
                        hasMediaExt,
                        hasAssertionExt,
                        hasIdentificationExt,
                        hasIdentifierExt),
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) r -> r != null);
  }

  private static ExtendedRecord toExtendedRecord(
      Row row,
      String[] occColumns,
      boolean hasMediaExt,
      boolean hasAssertionExt,
      boolean hasIdentificationExt,
      boolean hasIdentifierExt)
      throws IOException {

    String occurrenceId = RowTermMapper.safeGet(row, "occurrenceID");
    if (occurrenceId == null || occurrenceId.isEmpty()) {
      return null;
    }

    Map<String, String> coreTerms = RowTermMapper.toTermMap(row, occColumns);
    Map<String, List<Map<String, String>>> extensions = new HashMap<>();

    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasMediaExt,
        MediaExtensionBuilder.COL_MEDIA_EXT_JSON,
        MediaExtensionBuilder.ROW_TYPE_MULTIMEDIA);
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasAssertionExt,
        AssertionExtensionBuilder.COL_ASSERTION_EXT_JSON,
        AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasIdentificationExt,
        IdentificationExtensionBuilder.COL_IDENTIFICATION_EXT_JSON,
        IdentificationExtensionBuilder.ROW_TYPE_IDENTIFICATION);
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasIdentifierExt,
        IdentifierExtensionBuilder.COL_IDENTIFIER_EXT_JSON,
        IdentifierExtensionBuilder.ROW_TYPE_IDENTIFIER);

    return ExtendedRecord.newBuilder()
        .setId(occurrenceId)
        .setCoreId(null)
        .setCoreRowType(CORE_ROW_TYPE)
        .setCoreTerms(coreTerms)
        .setExtensions(extensions)
        .build();
  }
}
