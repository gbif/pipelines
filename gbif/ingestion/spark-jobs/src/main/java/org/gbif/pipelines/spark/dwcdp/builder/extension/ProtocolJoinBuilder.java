package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Resolves {@code protocol_pk}-referencing surrogate FK columns to the linked protocol's {@code
 * protocolDescription}.
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
  private static final String TEMP_RESOLVED_COLUMN = "__resolved_protocol_description";

  private ProtocolJoinBuilder() {}

  /**
   * Resolves {@code fkColumn} to a new column named {@code targetColumnName} holding the linked
   * protocol's {@code protocolDescription} — or, when the {@code protocol} table is absent, {@code
   * fkColumn}'s raw value under that same new name. Returns {@code df} unchanged if it has no
   * {@code fkColumn} column at all.
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
                withResolved.col(coalesceIntoColumn), withResolved.col(TEMP_RESOLVED_COLUMN)))
        .drop(TEMP_RESOLVED_COLUMN);
  }

  private static Dataset<Row> joinAndRename(
      Dataset<Row> df, Dataset<Row> protocolDf, String fkColumn, String targetColumnName) {
    Dataset<Row> protocolSelect =
        protocolDf.select(PROTOCOL_PK_COLUMN, PROTOCOL_DESCRIPTION_COLUMN);

    return df.join(
            protocolSelect,
            df.col(fkColumn).equalTo(protocolSelect.col(PROTOCOL_PK_COLUMN)),
            "left_outer")
        .drop(protocolSelect.col(PROTOCOL_PK_COLUMN))
        .drop(df.col(fkColumn))
        .withColumnRenamed(PROTOCOL_DESCRIPTION_COLUMN, targetColumnName);
  }
}
