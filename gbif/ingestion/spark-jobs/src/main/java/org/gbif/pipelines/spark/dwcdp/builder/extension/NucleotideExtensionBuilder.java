package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds DNA Derived Data extension Datasets ({@code Extension.DNA_DERIVED_DATA}) from {@code
 * nucleotide-analysis}, {@code nucleotide-sequence}, and {@code molecular-protocol}.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>nucleotide-analysis.nucleotideSequence_fk = nucleotide-sequence.pk (left outer) →
 *       sequence fields
 *   <li>nucleotide-analysis.molecularProtocol_fk = molecular-protocol.pk (left outer) → MIxS
 *       method/protocol fields
 *   <li>materialEntity_fk populated → resolved to occurrence via {@link
 *       MaterialJoinBuilder#singleMaterialOccurrenceLinks} — {@link #buildOccurrence}
 *   <li>event_fk populated, materialEntity_fk absent → resolved directly to event — {@link
 *       #buildEvent}
 * </ul>
 *
 * <p>A row with both FKs is attached only via {@link #buildOccurrence} — never duplicated onto the
 * event too. Term renames for {@code sequence}/{@code single_cell_lysis_appr}/{@code
 * single_cell_lysis_prot}: {@link DwcDpTermMappings}.
 *
 * <p><b>Deferred:</b> {@code nucleotide-analysis-assertion}, {@code molecular-protocol-assertion},
 * {@code molecular-protocol-agent-role}, {@code molecular-protocol-reference}, {@code
 * identification.nucleotideAnalysis_fk}/{@code nucleotideSequence_fk}. See mapping doc §4.9.
 */
@Slf4j
public class NucleotideExtensionBuilder {

  static final String TABLE_NUCLEOTIDE_ANALYSIS = "nucleotide-analysis";
  static final String TABLE_NUCLEOTIDE_SEQUENCE = "nucleotide-sequence";
  static final String TABLE_MOLECULAR_PROTOCOL = "molecular-protocol";

  /** Extension.DNA_DERIVED_DATA.getRowType() — derived from the real gbif-api enum. */
  public static final String ROW_TYPE_DNA_DERIVED_DATA = Extension.DNA_DERIVED_DATA.getRowType();

  public static final String COL_DNA_EXT_JSON = "dnaExtJson";

  private static final String NUCLEOTIDE_ANALYSIS_PK_COLUMN = "nucleotideAnalysis_pk";
  private static final String NUCLEOTIDE_SEQUENCE_FK_COLUMN = "nucleotideSequence_fk";
  private static final String NUCLEOTIDE_SEQUENCE_PK_COLUMN = "nucleotideSequence_pk";
  private static final String MOLECULAR_PROTOCOL_FK_COLUMN = "molecularProtocol_fk";
  private static final String MOLECULAR_PROTOCOL_PK_COLUMN = "molecularProtocol_pk";
  private static final String EVENT_FK_COLUMN = "event_fk";
  private static final String MATERIAL_ENTITY_FK_COLUMN = "materialEntity_fk";
  private static final String MATERIAL_ENTITY_PK_COLUMN = "materialEntity_pk";

  private NucleotideExtensionBuilder() {}

  /** eDNA/metabarcoding path: event_fk populated, materialEntity_fk absent. */
  public static Optional<Dataset<Row>> buildEvent(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> payloadOpt = resolveAnalysisPayload(loader);
    if (payloadOpt.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> payload = payloadOpt.get();
    List<String> columns = Arrays.asList(payload.columns());
    if (!columns.contains(EVENT_FK_COLUMN)) {
      log.debug("nucleotide-analysis has no event_fk column; skipping event-level DNA extension");
      return Optional.empty();
    }

    boolean hasMaterialFk = columns.contains(MATERIAL_ENTITY_FK_COLUMN);
    Dataset<Row> eventLinked =
        hasMaterialFk
            ? payload.filter(
                functions
                    .col(EVENT_FK_COLUMN)
                    .isNotNull()
                    .and(functions.col(MATERIAL_ENTITY_FK_COLUMN).isNull()))
            : payload.filter(functions.col(EVENT_FK_COLUMN).isNotNull());

    Optional<Dataset<Row>> eventDfOpt = loader.load("event");
    if (eventDfOpt.isEmpty()
        || !Arrays.asList(eventDfOpt.get().columns()).contains("eventID")
        || !Arrays.asList(eventDfOpt.get().columns()).contains("event_pk")) {
      log.debug(
          "event table absent or missing eventID/event_pk; skipping event-level DNA extension");
      return Optional.empty();
    }
    Dataset<Row> eventDf = eventDfOpt.get();

    Dataset<Row> resolved =
        eventLinked
            .join(
                eventDf.select("event_pk", "eventID"),
                eventLinked.col(EVENT_FK_COLUMN).equalTo(eventDf.col("event_pk")),
                "inner")
            .drop(eventDf.col("event_pk"))
            .drop(eventLinked.col(EVENT_FK_COLUMN));
    if (hasMaterialFk) {
      resolved = resolved.drop(MATERIAL_ENTITY_FK_COLUMN);
    }

    if (resolved.isEmpty()) {
      log.debug("No nucleotide-analysis rows resolved to an event; skipping DNA extension");
      return Optional.empty();
    }

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, resolved, resolved.columns(), "eventID", COL_DNA_EXT_JSON));
  }

  /** Physical-specimen path: materialEntity_fk resolved via {@link MaterialJoinBuilder#singleMaterialOccurrenceLinks}. */
  public static Optional<Dataset<Row>> buildOccurrence(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> payloadOpt = resolveAnalysisPayload(loader);
    if (payloadOpt.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> payload = payloadOpt.get();
    if (!Arrays.asList(payload.columns()).contains(MATERIAL_ENTITY_FK_COLUMN)) {
      log.debug(
          "nucleotide-analysis has no materialEntity_fk column; skipping occurrence-level DNA "
              + "extension");
      return Optional.empty();
    }

    Optional<Dataset<Row>> materialLinksOpt =
        MaterialJoinBuilder.singleMaterialOccurrenceLinks(loader);
    if (materialLinksOpt.isEmpty()) {
      log.debug(
          "No single-material-per-occurrence links available; skipping occurrence-level DNA "
              + "extension");
      return Optional.empty();
    }
    Dataset<Row> materialLinks = materialLinksOpt.get();

    Dataset<Row> materialLinked =
        payload.filter(functions.col(MATERIAL_ENTITY_FK_COLUMN).isNotNull());
    Dataset<Row> resolved =
        materialLinked
            .join(
                materialLinks,
                materialLinked
                    .col(MATERIAL_ENTITY_FK_COLUMN)
                    .equalTo(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN)),
                "inner")
            .drop(materialLinks.col(MATERIAL_ENTITY_PK_COLUMN))
            .drop(materialLinked.col(MATERIAL_ENTITY_FK_COLUMN));
    if (Arrays.asList(resolved.columns()).contains(EVENT_FK_COLUMN)) {
      resolved = resolved.drop(EVENT_FK_COLUMN);
    }

    if (resolved.isEmpty()) {
      log.debug(
          "No nucleotide-analysis rows resolved to a single-material occurrence; skipping DNA "
              + "extension");
      return Optional.empty();
    }

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, resolved, resolved.columns(), "occurrenceID", COL_DNA_EXT_JSON));
  }

  /** {@code event_fk}/{@code materialEntity_fk} deliberately retained — {@link #buildEvent}/{@link #buildOccurrence} each pick their own subset before dropping. */
  private static Optional<Dataset<Row>> resolveAnalysisPayload(TableLoader loader) {
    Optional<Dataset<Row>> analysisDfOpt = loader.load(TABLE_NUCLEOTIDE_ANALYSIS);
    if (analysisDfOpt.isEmpty()) {
      log.debug("No nucleotide-analysis table present; skipping DNA extension entirely");
      return Optional.empty();
    }

    Dataset<Row> withSequence =
        leftJoinDropFk(
            loader,
            analysisDfOpt.get(),
            TABLE_NUCLEOTIDE_SEQUENCE,
            NUCLEOTIDE_SEQUENCE_FK_COLUMN,
            NUCLEOTIDE_SEQUENCE_PK_COLUMN);
    Dataset<Row> withProtocol =
        leftJoinDropFk(
            loader,
            withSequence,
            TABLE_MOLECULAR_PROTOCOL,
            MOLECULAR_PROTOCOL_FK_COLUMN,
            MOLECULAR_PROTOCOL_PK_COLUMN);

    return Optional.of(withProtocol.drop(NUCLEOTIDE_ANALYSIS_PK_COLUMN));
  }

  /** Absent child table → FK simply dropped (unlike {@link ProtocolJoinBuilder}'s raw-FK fallback — no DwC term this surrogate ID could stand in for). */
  private static Dataset<Row> leftJoinDropFk(
      TableLoader loader,
      Dataset<Row> left,
      String childTable,
      String fkColumn,
      String childPkColumn) {
    if (!Arrays.asList(left.columns()).contains(fkColumn)) {
      return left;
    }

    Optional<Dataset<Row>> childDfOpt = loader.load(childTable);
    if (childDfOpt.isEmpty()) {
      log.debug("No {} table present; dropping unresolved {}", childTable, fkColumn);
      return left.drop(fkColumn);
    }

    Dataset<Row> childDf = childDfOpt.get();
    if (!Arrays.asList(childDf.columns()).contains(childPkColumn)) {
      log.warn(
          "{} table is missing {}; dropping unresolved {}", childTable, childPkColumn, fkColumn);
      return left.drop(fkColumn);
    }

    return left.join(childDf, left.col(fkColumn).equalTo(childDf.col(childPkColumn)), "left_outer")
        .drop(childDf.col(childPkColumn))
        .drop(left.col(fkColumn));
  }
}
