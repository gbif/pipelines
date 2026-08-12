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
import org.gbif.pipelines.spark.dwcdp.builder.extension.AgentJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentificationExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentificationJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.IdentifierExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialGeologicalContextJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialProtocolJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MaterialProvenanceJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.MediaExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.NucleotideExtensionBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.OrganismJoinBuilder;
import org.gbif.pipelines.spark.dwcdp.builder.extension.ProtocolJoinBuilder;
import org.gbif.pipelines.spark.util.DatasetJoins;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds a distributed Dataset of occurrence-core {@link ExtendedRecord}s from DwC-DP Parquet
 * files.
 *
 * <p>Enriches via {@link OrganismJoinBuilder}, {@link IdentificationJoinBuilder}, {@link
 * MaterialJoinBuilder} (+ its {@link MaterialGeologicalContextJoinBuilder}/{@link
 * MaterialProvenanceJoinBuilder}/{@link MaterialProtocolJoinBuilder} satellites), {@link
 * ProtocolJoinBuilder}, {@link AgentJoinBuilder}; attaches extensions via {@link
 * MediaExtensionBuilder}, {@link AssertionExtensionBuilder}, {@link
 * IdentificationExtensionBuilder}, {@link IdentifierExtensionBuilder}, {@link
 * NucleotideExtensionBuilder}. Full call sequence and skip conditions: mapping doc §3.1.
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

    // occurrence_pk is required+unique per the DwC-DP profile; occurrenceID is not. Falling back
    // once here, at the loader level, means every downstream consumer of "occurrence" (this
    // method's own occurrenceDf below, plus OrganismJoinBuilder/IdentificationJoinBuilder/
    // MaterialJoinBuilder/MediaExtensionBuilder/AssertionExtensionBuilder/
    // IdentificationExtensionBuilder/IdentifierExtensionBuilder/NucleotideExtensionBuilder, which
    // each independently reload "occurrence" from this same loader) sees a usable occurrenceID
    // automatically. See CoreBuilderSupport#withIdFallback.
    loader =
        CoreBuilderSupport.withIdFallback(
            loader,
            "occurrence",
            OCCURRENCE_PK_COLUMN,
            "occurrenceID",
            CoreBuilderSupport.OCCURRENCE_URN_PREFIX);

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
    enriched = MaterialGeologicalContextJoinBuilder.enrichOccurrences(loader, enriched);
    enriched = MaterialProvenanceJoinBuilder.enrichOccurrences(loader, enriched);
    enriched =
        ProtocolJoinBuilder.resolveProtocolFk(
            loader, enriched, "occurrenceProtocol_fk", "samplingProtocol");
    enriched = MaterialProtocolJoinBuilder.enrichOccurrences(loader, enriched);
    enriched =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            loader, enriched, "recordedByID", "recordedBy");
    enriched =
        AgentJoinBuilder.resolveAgentNameCoalesceInto(
            loader, enriched, "identifiedByID", "identifiedBy");
    enriched = enriched.drop(OCCURRENCE_PK_COLUMN);

    Optional<Dataset<Row>> mediaExtDf =
        MediaExtensionBuilder.buildOccurrenceMediaExtension(spark, loader);
    Optional<Dataset<Row>> assertionExtDf =
        AssertionExtensionBuilder.buildOccurrenceAssertionExtension(spark, loader);
    Optional<Dataset<Row>> identificationExtDf =
        IdentificationExtensionBuilder.build(spark, loader);
    Optional<Dataset<Row>> identifierExtDf = IdentifierExtensionBuilder.build(spark, loader);
    Optional<Dataset<Row>> dnaExtDf = NucleotideExtensionBuilder.buildOccurrence(spark, loader);

    Dataset<Row> joined = enriched;
    joined = DatasetJoins.leftJoinIfPresent(joined, mediaExtDf, "occurrenceID");
    joined = DatasetJoins.leftJoinIfPresent(joined, assertionExtDf, "occurrenceID");
    joined = DatasetJoins.leftJoinIfPresent(joined, identificationExtDf, "occurrenceID");
    joined = DatasetJoins.leftJoinIfPresent(joined, identifierExtDf, "occurrenceID");
    joined = DatasetJoins.leftJoinIfPresent(joined, dnaExtDf, "occurrenceID");

    final String[] occColumns = enriched.columns();
    final boolean hasMediaExt = mediaExtDf.isPresent();
    final boolean hasAssertionExt = assertionExtDf.isPresent();
    final boolean hasIdentificationExt = identificationExtDf.isPresent();
    final boolean hasIdentifierExt = identifierExtDf.isPresent();
    final boolean hasDnaExt = dnaExtDf.isPresent();

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
                        hasIdentifierExt,
                        hasDnaExt),
            Encoders.bean(ExtendedRecord.class))
        .filter((FilterFunction<ExtendedRecord>) r -> r != null);
  }

  private static ExtendedRecord toExtendedRecord(
      Row row,
      String[] occColumns,
      boolean hasMediaExt,
      boolean hasAssertionExt,
      boolean hasIdentificationExt,
      boolean hasIdentifierExt,
      boolean hasDnaExt)
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
    CoreBuilderSupport.addExtensionIfPresent(
        row,
        extensions,
        hasDnaExt,
        NucleotideExtensionBuilder.COL_DNA_EXT_JSON,
        NucleotideExtensionBuilder.ROW_TYPE_DNA_DERIVED_DATA);

    return ExtendedRecord.newBuilder()
        .setId(occurrenceId)
        .setCoreId(null)
        .setCoreRowType(CORE_ROW_TYPE)
        .setCoreTerms(coreTerms)
        .setExtensions(extensions)
        .build();
  }
}
