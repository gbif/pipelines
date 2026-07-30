package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Resolves {@code protocol_pk}-referencing surrogate FK columns to the linked protocol's display
 * label.
 *
 * <p>DwC-DP carries several protocol references as bare surrogate FKs with no accompanying text:
 * {@code event.eventProtocol_fk}, {@code occurrence.occurrenceProtocol_fk}. Left unresolved, these
 * fall through {@code TermResolver} under their own raw column names — meaningless internal IDs
 * masquerading as term values, the same class of leak {@code AssertionExtensionBuilder} already
 * solves for {@code assertionProtocol_fk}. This builder applies the same policy generally: resolve
 * via the {@code protocol} table when present, fall back to the raw FK value when it's absent
 * (better than nothing — mirrors {@code AssertionExtensionBuilder}'s own tested fallback for {@code
 * assertionProtocol_fk}), never silently drop the column.
 *
 * <p>{@code event.georeferenceProtocol_fk} is a related but distinct case, handled by {@link
 * #resolveProtocolFkCoalesceInto}: {@code event} already carries a literal {@code
 * georeferenceProtocol} text field alongside the FK, so publisher-supplied free text must win where
 * present — the FK is only a supplementary source used to fill gaps, never to overwrite.
 *
 * <p>The mapping of {@code eventProtocol_fk}/{@code occurrenceProtocol_fk} to {@code
 * dwc:samplingProtocol} is this project's best inference from the DwC-DP schema — no DwC-DP field
 * maps to {@code dwc:samplingProtocol} otherwise, but it hasn't been independently confirmed
 * against a mapping document the way the media field renames were.
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

  private ProtocolJoinBuilder() {}

  /**
   * Resolves {@code fkColumn} to a new column named {@code targetColumnName} holding the linked
   * protocol's display label — or, when the {@code protocol} table is absent, {@code fkColumn}'s
   * raw value under that same new name. Returns {@code df} unchanged if it has no {@code fkColumn}
   * column at all.
   *
   * @param loader table loader — returns {@link Optional#empty()} when {@code protocol} is absent
   * @param df the Dataset to resolve the FK on (typically {@code event} or {@code occurrence})
   * @param fkColumn the surrogate FK column to resolve, e.g. {@code "eventProtocol_fk"}
   * @param targetColumnName the new column name to hold the resolved value, e.g. {@code
   *     "samplingProtocol"}
   */
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

  /**
   * Resolves {@code fkColumn} the same way as {@link #resolveProtocolFk}, but instead of creating a
   * new column, coalesces the resolved (or fallback raw) value into an <em>existing</em> column
   * ({@code coalesceIntoColumn}) — only where that column is currently null. An existing
   * publisher-supplied value in {@code coalesceIntoColumn} is never overwritten.
   *
   * <p>Returns {@code df} unchanged if it has no {@code fkColumn} column at all. If {@code df} also
   * has no {@code coalesceIntoColumn} column, one is created holding just the resolved/fallback
   * value (nothing to coalesce against).
   *
   * @param coalesceIntoColumn an existing text column that should take precedence when populated,
   *     e.g. {@code "georeferenceProtocol"}
   */
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

  /**
   * Resolves a protocol junction table into one deterministic pipe-delimited display-label list per
   * parent. When the protocol lookup table is absent, protocol FK values are retained as the same
   * fallback used by {@link #resolveProtocolFk}; when it is present, dangling protocol FKs do not
   * contribute a value.
   *
   * <p>The returned Dataset contains {@code parentIdColumn} and an internal display-label-list
   * column intended for {@link #mergeJunctionProtocolsInto}. Empty signals that a required table or
   * column is absent, rather than an empty junction table.
   */
  public static Optional<Dataset<Row>> aggregateJunctionProtocolDescriptions(
      TableLoader loader,
      String junctionTable,
      String junctionParentFkColumn,
      String parentTable,
      String parentPkColumn,
      String parentIdColumn) {
    Optional<Dataset<Row>> junctionDfOpt = loader.load(junctionTable);
    Optional<Dataset<Row>> parentDfOpt = loader.load(parentTable);
    if (junctionDfOpt.isEmpty() || parentDfOpt.isEmpty()) {
      return Optional.empty();
    }

    Dataset<Row> junctionDf = junctionDfOpt.get();
    Dataset<Row> parentDf = parentDfOpt.get();
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
      descriptions = links.withColumnRenamed(PROTOCOL_FK_COLUMN, PROTOCOL_DESCRIPTION_COLUMN);
    } else {
      Dataset<Row> protocolDf = protocolDfOpt.get();
      if (!hasColumns(protocolDf, PROTOCOL_PK_COLUMN)) {
        log.warn("protocol table is missing its primary-key column; using protocol FK fallback");
        descriptions = links.withColumnRenamed(PROTOCOL_FK_COLUMN, PROTOCOL_DESCRIPTION_COLUMN);
      } else {
        Dataset<Row> protocolSelect =
            protocolDf.select(
                protocolDf.col(PROTOCOL_PK_COLUMN),
                protocolDisplayColumn(protocolDf).as(PROTOCOL_DESCRIPTION_COLUMN));
        descriptions =
            links
                .join(
                    protocolSelect,
                    links.col(PROTOCOL_FK_COLUMN).equalTo(protocolSelect.col(PROTOCOL_PK_COLUMN)),
                    "inner")
                .select(links.col(parentIdColumn), protocolSelect.col(PROTOCOL_DESCRIPTION_COLUMN));
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

  /**
   * Merges aggregated junction protocol display labels into a pipe-delimited target field. Existing
   * direct values are preserved, duplicate values are removed, and all values are sorted for
   * deterministic output.
   */
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

  /**
   * Builds the human-readable representation used by DwC-A's scalar protocol fields. A named
   * protocol is represented as {@code "type: name"} where the type is available; an unnamed
   * protocol falls back to its free-text description.
   */
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
