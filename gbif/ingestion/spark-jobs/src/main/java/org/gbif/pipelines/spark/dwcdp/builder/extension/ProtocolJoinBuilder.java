package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Resolves {@code protocol_pk}-referencing FK columns and junction tables to the linked protocol's
 * display label ({@code "type: name"}, or description as fallback).
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>event.eventProtocol_fk / occurrence.occurrenceProtocol_fk = protocol.protocol_pk (left
 *       outer, direct) → samplingProtocol — {@link #resolveProtocolFk}
 *   <li>event.georeferenceProtocol_fk = protocol.protocol_pk (left outer, coalesce-if-null) →
 *       georeferenceProtocol — {@link #resolveProtocolFkCoalesceInto}
 *   <li>event-protocol / survey-protocol / material-protocol = protocol.protocol_pk (inner,
 *       junction, optional protocolType filter) → aggregated pipe-delimited list — {@link
 *       #aggregateJunctionProtocolDescriptions} + {@link #mergeJunctionProtocolsInto}
 * </ul>
 *
 * <p><b>Fallback:</b> protocol table absent/malformed → raw FK value kept, never dropped.
 *
 * <p>See mapping doc §4.2 for design rationale and known gaps.
 */
@Slf4j
public class ProtocolJoinBuilder {

  public static final String TABLE_PROTOCOL = "protocol";
  static final String PROTOCOL_PK_COLUMN = "protocol_pk";
  static final String PROTOCOL_DESCRIPTION_COLUMN = "protocolDescription";
  static final String PROTOCOL_TYPE_COLUMN = "protocolType";
  static final String PROTOCOL_NAME_COLUMN = "protocolName";
  private static final String PROTOCOL_FK_COLUMN = "protocol_fk";
  private static final String AGGREGATED_PROTOCOLS_COLUMN = "__aggregated_protocol_descriptions";
  private static final String AGGREGATED_JOIN_KEY_COLUMN = "__aggregated_protocol_parent_key";
  private static final String TEMP_RESOLVED_COLUMN = "__resolved_protocol_description";
  private static final String FUNNEL_PROTOCOL_PK_ALIAS = "__funnel_protocol_pk";

  /**
   * Controlled-vocabulary {@code protocolType} values (compared case-insensitively) that identify a
   * protocol as a georeferencing protocol, for routing into {@code dwc:georeferenceProtocol} rather
   * than (or in addition to) {@code dwc:samplingProtocol}. Both spellings seen in DwC-DP packages
   * are accepted; extend this set if a dataset uses a different literal.
   */
  public static final Set<String> GEOREFERENCE_PROTOCOL_TYPES =
      Set.of("georeference", "georeferencing");

  private ProtocolJoinBuilder() {}

  /** {@code df} unchanged if it has no {@code fkColumn}. */
  public static Dataset<Row> resolveProtocolFk(
      TableLoader loader, Dataset<Row> df, String fkColumn, String targetColumnName) {
    if (!Arrays.asList(df.columns()).contains(fkColumn)) {
      return df;
    }

    Optional<Dataset<Row>> protocolDfOpt = loader.load(TABLE_PROTOCOL);
    if (protocolDfOpt.isEmpty()) {
      log.debug(
          "No protocol table present; keeping raw {} value as fallback under {}",
          fkColumn,
          targetColumnName);
      return df.withColumnRenamed(fkColumn, targetColumnName);
    }

    return joinAndRename(df, protocolDfOpt.get(), fkColumn, targetColumnName);
  }

  /** Same resolution as {@link #resolveProtocolFk}, coalesced into an existing column — publisher value wins. */
  public static Dataset<Row> resolveProtocolFkCoalesceInto(
      TableLoader loader, Dataset<Row> df, String fkColumn, String coalesceIntoColumn) {
    if (!Arrays.asList(df.columns()).contains(fkColumn)) {
      return df;
    }

    Optional<Dataset<Row>> protocolDfOpt = loader.load(TABLE_PROTOCOL);
    Dataset<Row> withResolved =
        protocolDfOpt.isEmpty()
            ? df.withColumnRenamed(fkColumn, TEMP_RESOLVED_COLUMN)
            : joinAndRename(df, protocolDfOpt.get(), fkColumn, TEMP_RESOLVED_COLUMN);

    if (!Arrays.asList(df.columns()).contains(coalesceIntoColumn)) {
      return withResolved.withColumnRenamed(TEMP_RESOLVED_COLUMN, coalesceIntoColumn);
    }

    return withResolved
        .withColumn(
            coalesceIntoColumn,
            functions.coalesce(
                withResolved.col(quotedColumn(coalesceIntoColumn)),
                withResolved.col(TEMP_RESOLVED_COLUMN)))
        .drop(TEMP_RESOLVED_COLUMN);
  }

  /** One row per {@code parentIdColumn}, ready for {@link #mergeJunctionProtocolsInto}. Empty means a required table/column is absent, not "no matches." */
  public static Optional<Dataset<Row>> aggregateJunctionProtocolDescriptions(
      TableLoader loader,
      String junctionTable,
      String junctionParentFkColumn,
      String parentTable,
      String parentPkColumn,
      String parentIdColumn) {
    Optional<Dataset<Row>> parentDfOpt = loader.load(parentTable);
    if (parentDfOpt.isEmpty()) {
      return Optional.empty();
    }
    return aggregateJunctionProtocolDescriptions(
        loader,
        junctionTable,
        junctionParentFkColumn,
        parentDfOpt.get(),
        parentPkColumn,
        parentIdColumn,
        null);
  }

  /**
   * Same as the {@code (TableLoader, String, String, String, String, String)} overload, but takes
   * an already-resolved {@code parentDf} (e.g. survey-protocol needs survey resolved to its
   * owning event first) and an optional {@code protocolType} filter.
   *
   * @param allowedProtocolTypesLowercase null = every linked protocol contributes; non-empty =
   *     only matching {@code protocolType} contributes (e.g. {@link #GEOREFERENCE_PROTOCOL_TYPES}).
   *     A filter that can't be evaluated returns empty rather than guessing.
   */
  public static Optional<Dataset<Row>> aggregateJunctionProtocolDescriptions(
      TableLoader loader,
      String junctionTable,
      String junctionParentFkColumn,
      Dataset<Row> parentDf,
      String parentPkColumn,
      String parentIdColumn,
      Set<String> allowedProtocolTypesLowercase) {
    boolean hasTypeFilter =
        allowedProtocolTypesLowercase != null && !allowedProtocolTypesLowercase.isEmpty();

    Optional<Dataset<Row>> junctionDfOpt = loader.load(junctionTable);
    if (junctionDfOpt.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> junctionDf = junctionDfOpt.get();
    if (!hasColumns(junctionDf, junctionParentFkColumn, PROTOCOL_FK_COLUMN)
        || !hasColumns(parentDf, parentPkColumn, parentIdColumn)) {
      log.warn("Cannot resolve {}: required junction or parent columns are absent", junctionTable);
      return Optional.empty();
    }

    Dataset<Row> links =
        junctionDf
            .join(
                parentDf.select(parentPkColumn, parentIdColumn),
                junctionDf.col(junctionParentFkColumn).equalTo(parentDf.col(parentPkColumn)),
                "inner")
            .select(parentDf.col(parentIdColumn), junctionDf.col(PROTOCOL_FK_COLUMN));

    Optional<Dataset<Row>> protocolDfOpt = loader.load(TABLE_PROTOCOL);
    Dataset<Row> descriptions;
    if (protocolDfOpt.isEmpty()) {
      if (hasTypeFilter) {
        log.debug(
            "protocol table absent; cannot apply protocolType filter for {}, contributing nothing",
            junctionTable);
        return Optional.empty();
      }
      descriptions = links.withColumnRenamed(PROTOCOL_FK_COLUMN, PROTOCOL_DESCRIPTION_COLUMN);
    } else {
      Dataset<Row> protocolDf = protocolDfOpt.get();
      boolean hasType = hasColumns(protocolDf, PROTOCOL_TYPE_COLUMN);
      if (!hasColumns(protocolDf, PROTOCOL_PK_COLUMN) || (hasTypeFilter && !hasType)) {
        if (hasTypeFilter) {
          log.debug(
              "protocol table has no {} column; cannot apply protocolType filter for {}, "
                  + "contributing nothing",
              PROTOCOL_TYPE_COLUMN,
              junctionTable);
          return Optional.empty();
        }
        log.warn("protocol table is missing its primary-key column; using protocol FK fallback");
        descriptions = links.withColumnRenamed(PROTOCOL_FK_COLUMN, PROTOCOL_DESCRIPTION_COLUMN);
      } else {
        Dataset<Row> protocolSelect =
            hasType
                ? protocolDf.select(
                    protocolDf.col(PROTOCOL_PK_COLUMN),
                    protocolDf.col(PROTOCOL_TYPE_COLUMN),
                    protocolDisplayColumn(protocolDf).as(PROTOCOL_DESCRIPTION_COLUMN))
                : protocolDf.select(
                    protocolDf.col(PROTOCOL_PK_COLUMN),
                    protocolDisplayColumn(protocolDf).as(PROTOCOL_DESCRIPTION_COLUMN));

        Dataset<Row> joined =
            links.join(
                protocolSelect,
                links.col(PROTOCOL_FK_COLUMN).equalTo(protocolSelect.col(PROTOCOL_PK_COLUMN)),
                "inner");

        if (hasTypeFilter) {
          joined =
              joined.filter(
                  functions
                      .lower(joined.col(PROTOCOL_TYPE_COLUMN))
                      .isin(allowedProtocolTypesLowercase.toArray()));
        }

        descriptions =
            joined.select(
                links.col(parentIdColumn), protocolSelect.col(PROTOCOL_DESCRIPTION_COLUMN));
      }
    }

    Dataset<Row> aggregated =
        descriptions
            .filter(functions.col(PROTOCOL_DESCRIPTION_COLUMN).isNotNull())
            .groupBy(functions.col(parentIdColumn))
            .agg(
                functions
                    .array_join(
                        functions.array_sort(
                            functions.collect_set(functions.col(PROTOCOL_DESCRIPTION_COLUMN))),
                        "|")
                    .as(AGGREGATED_PROTOCOLS_COLUMN));
    return Optional.of(aggregated);
  }

  /** Merges {@link #aggregateJunctionProtocolDescriptions} output into {@code targetColumn}, deduped and sorted. */
  public static Dataset<Row> mergeJunctionProtocolsInto(
      Dataset<Row> df,
      Optional<Dataset<Row>> aggregatedProtocols,
      String dfKeyColumn,
      String aggregatedKeyColumn,
      String targetColumn) {
    if (aggregatedProtocols.isEmpty() || !Arrays.asList(df.columns()).contains(dfKeyColumn)) {
      return df;
    }

    Dataset<Row> aggregate =
        aggregatedProtocols
            .get()
            .withColumnRenamed(aggregatedKeyColumn, AGGREGATED_JOIN_KEY_COLUMN);
    Dataset<Row> joined =
        df.join(
            aggregate,
            df.col(dfKeyColumn).equalTo(aggregate.col(AGGREGATED_JOIN_KEY_COLUMN)),
            "left_outer");

    if (!Arrays.asList(joined.columns()).contains(targetColumn)) {
      return joined
          .withColumnRenamed(AGGREGATED_PROTOCOLS_COLUMN, targetColumn)
          .drop(AGGREGATED_JOIN_KEY_COLUMN);
    }

    var values =
        functions.filter(
            functions.array_union(
                functions.split(
                    functions.coalesce(joined.col(targetColumn), functions.lit("")), "\\|"),
                functions.split(
                    functions.coalesce(joined.col(AGGREGATED_PROTOCOLS_COLUMN), functions.lit("")),
                    "\\|")),
            value -> value.isNotNull().and(functions.length(value).gt(0)));
    var merged = functions.array_join(functions.array_sort(values), "|");

    return joined
        .withColumn(
            targetColumn,
            functions
                .when(functions.length(merged).equalTo(0), functions.lit(null))
                .otherwise(merged))
        .drop(AGGREGATED_JOIN_KEY_COLUMN)
        .drop(AGGREGATED_PROTOCOLS_COLUMN);
  }

  private static boolean hasColumns(Dataset<Row> df, String... columns) {
    return Arrays.asList(df.columns()).containsAll(Arrays.asList(columns));
  }

  /** Quotes a Spark column identifier so qualified term URIs remain a single field name. */
  private static String quotedColumn(String columnName) {
    return "`" + columnName.replace("`", "``") + "`";
  }

  /**
   * Covers only the direct-FK path ({@link #resolveProtocolFk}) — not the coalesce path or the
   * junction-aggregation path, which have different bucket shapes. Buckets: table absent/malformed
   * (raw FK fallback) / resolved / dangling FK (dropped).
   */
  public static Optional<JoinFunnel> computeFunnel(
      TableLoader loader, String coreTable, String fkColumn, String targetColumnName) {
    Optional<Dataset<Row>> coreDfOpt = loader.load(coreTable);
    if (coreDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> coreDf = coreDfOpt.get();
    if (!Arrays.asList(coreDf.columns()).contains(fkColumn)) {
      return Optional.empty();
    }

    String label =
        "ProtocolJoinBuilder (" + coreTable + "." + fkColumn + " -> " + targetColumnName + ")";
    long candidates = coreDf.filter(functions.col(fkColumn).isNotNull()).count();
    if (candidates == 0L) {
      return Optional.of(
          new JoinFunnel(label, List.of(bucket("candidates (" + fkColumn + " set)", 0L))));
    }

    Optional<Dataset<Row>> protocolDfOpt = loader.load(TABLE_PROTOCOL);
    if (protocolDfOpt.isEmpty()) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  bucket("candidates (" + fkColumn + " set)", candidates),
                  bucket("protocol table absent, raw FK kept as fallback", candidates))));
    }

    Dataset<Row> protocolDf = protocolDfOpt.get();
    if (!hasColumns(protocolDf, PROTOCOL_PK_COLUMN)) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  bucket("candidates (" + fkColumn + " set)", candidates),
                  bucket(
                      "protocol table missing primary key, raw FK kept as fallback", candidates))));
    }

    Dataset<Row> protocolIds =
        protocolDf
            .select(functions.col(PROTOCOL_PK_COLUMN).as(FUNNEL_PROTOCOL_PK_ALIAS))
            .distinct();
    long resolved =
        coreDf
            .filter(functions.col(fkColumn).isNotNull())
            .join(
                protocolIds,
                coreDf.col(fkColumn).equalTo(protocolIds.col(FUNNEL_PROTOCOL_PK_ALIAS)),
                "left_semi")
            .count();
    long danglingFk = candidates - resolved;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                bucket("candidates (" + fkColumn + " set)", candidates),
                bucket("resolved to protocol description", resolved),
                bucket("dangling FK, no matching protocol_pk (value dropped)", danglingFk))));
  }

  private static JoinFunnel.Bucket bucket(String name, long count) {
    return new JoinFunnel.Bucket(name, count);
  }

  private static Dataset<Row> joinAndRename(
      Dataset<Row> df, Dataset<Row> protocolDf, String fkColumn, String targetColumnName) {
    if (!hasColumns(protocolDf, PROTOCOL_PK_COLUMN)) {
      log.warn(
          "protocol table is missing its primary-key column; keeping raw {} value as fallback",
          fkColumn);
      return df.withColumnRenamed(fkColumn, targetColumnName);
    }

    Dataset<Row> protocolSelect =
        protocolDf.select(
            protocolDf.col(PROTOCOL_PK_COLUMN),
            protocolDisplayColumn(protocolDf).as(targetColumnName));

    return df.join(
            protocolSelect,
            df.col(fkColumn).equalTo(protocolSelect.col(PROTOCOL_PK_COLUMN)),
            "left_outer")
        .drop(protocolSelect.col(PROTOCOL_PK_COLUMN))
        .drop(df.col(fkColumn));
  }

  /** {@code "type: name"} where available, else {@code protocolDescription}. */
  private static Column protocolDisplayColumn(Dataset<Row> protocolDf) {
    boolean hasName = Arrays.asList(protocolDf.columns()).contains(PROTOCOL_NAME_COLUMN);
    boolean hasType = Arrays.asList(protocolDf.columns()).contains(PROTOCOL_TYPE_COLUMN);
    boolean hasDescription =
        Arrays.asList(protocolDf.columns()).contains(PROTOCOL_DESCRIPTION_COLUMN);

    if (!hasName) {
      return hasDescription
          ? protocolDf.col(PROTOCOL_DESCRIPTION_COLUMN)
          : functions.lit(null).cast("string");
    }

    Column name = protocolDf.col(PROTOCOL_NAME_COLUMN);
    Column namedDisplay =
        hasType ? functions.concat_ws(": ", protocolDf.col(PROTOCOL_TYPE_COLUMN), name) : name;

    return hasDescription
        ? functions
            .when(name.isNotNull(), namedDisplay)
            .otherwise(protocolDf.col(PROTOCOL_DESCRIPTION_COLUMN))
        : functions
            .when(name.isNotNull(), namedDisplay)
            .otherwise(functions.lit(null).cast("string"));
  }
}
