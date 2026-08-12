package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Resolves {@code agent.agentID}-referencing FK columns to the linked agent's {@code
 * preferredAgentName}.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>{@code idColumn} = agent.agentID (left outer, coalesce-if-null) → {@code nameColumn}
 * </ul>
 *
 * <p><b>Wired for:</b> event.eventConductedByID→eventConductedBy,
 * event.georeferencedByID→georeferencedBy, occurrence.recordedByID→recordedBy,
 * occurrence.identifiedByID→identifiedBy.
 *
 * <p><b>Deferred:</b> {@code *-agent-role} junction tables; {@code
 * identification.identifiedByID}. See mapping doc §4.1 for rationale and design decisions.
 */
@Slf4j
public class AgentJoinBuilder {

  public static final String TABLE_AGENT = "agent";
  static final String AGENT_ID_COLUMN = "agentID";
  static final String AGENT_NAME_COLUMN = "preferredAgentName";
  private static final String AGENT_JOIN_ALIAS_COLUMN = "__agent_join_id";
  private static final String TEMP_RESOLVED_NAME_COLUMN = "__resolved_agent_name";

  private AgentJoinBuilder() {}

  /**
   * {@code idColumn} left untouched; {@code df} unchanged if {@code idColumn} or {@code agent} is
   * absent.
   */
  public static Dataset<Row> resolveAgentNameCoalesceInto(
      TableLoader loader, Dataset<Row> df, String idColumn, String nameColumn) {
    if (!Arrays.asList(df.columns()).contains(idColumn)) {
      return df;
    }

    Optional<Dataset<Row>> agentDfOpt = loader.load(TABLE_AGENT);
    if (agentDfOpt.isEmpty()) {
      log.debug(
          "No agent table present; leaving {} unresolved ({} kept as-is)", nameColumn, idColumn);
      return df;
    }

    Dataset<Row> agentDf = agentDfOpt.get();
    if (!Arrays.asList(agentDf.columns()).contains(AGENT_ID_COLUMN)) {
      log.warn("agent table is missing {}; skipping {} resolution", AGENT_ID_COLUMN, nameColumn);
      return df;
    }

    Dataset<Row> agentSelect =
        agentDf.select(
            agentDf.col(AGENT_ID_COLUMN).as(AGENT_JOIN_ALIAS_COLUMN),
            agentDisplayColumn(agentDf).as(TEMP_RESOLVED_NAME_COLUMN));

    Dataset<Row> joined =
        df.join(
                agentSelect,
                df.col(idColumn).equalTo(agentSelect.col(AGENT_JOIN_ALIAS_COLUMN)),
                "left_outer")
            .drop(AGENT_JOIN_ALIAS_COLUMN);

    if (!Arrays.asList(df.columns()).contains(nameColumn)) {
      return joined.withColumnRenamed(TEMP_RESOLVED_NAME_COLUMN, nameColumn);
    }

    return joined
        .withColumn(
            nameColumn,
            functions.coalesce(
                joined.col(quotedColumn(nameColumn)), joined.col(TEMP_RESOLVED_NAME_COLUMN)))
        .drop(TEMP_RESOLVED_NAME_COLUMN);
  }

  /**
   * {@code agent.preferredAgentName} if the column is present, else a typed {@code null} literal.
   * {@code agent} has no secondary display fallback field the way {@code protocol} does — its only
   * other field, {@code agentRemarks}, is free-text commentary, not a name substitute.
   */
  private static Column agentDisplayColumn(Dataset<Row> agentDf) {
    return Arrays.asList(agentDf.columns()).contains(AGENT_NAME_COLUMN)
        ? agentDf.col(AGENT_NAME_COLUMN)
        : functions.lit(null).cast("string");
  }

  /**
   * Buckets: already had {@code nameColumn} / resolved / no matching {@code agentID}, mirroring
   * {@link #resolveAgentNameCoalesceInto}'s decision logic.
   *
   * <p>Reloads {@code coreTable} fresh — valid here since {@code idColumn}/{@code nameColumn} are
   * raw DwC-DP columns, not introduced by an earlier step in the chain.
   */
  public static Optional<JoinFunnel> computeFunnel(
      TableLoader loader, String coreTable, String idColumn, String nameColumn) {
    Optional<Dataset<Row>> coreDfOpt = loader.load(coreTable);
    if (coreDfOpt.isEmpty()) {
      return Optional.empty();
    }
    Dataset<Row> coreDf = coreDfOpt.get();
    if (!Arrays.asList(coreDf.columns()).contains(idColumn)) {
      return Optional.empty();
    }

    String label = "AgentJoinBuilder (" + coreTable + "." + idColumn + " -> " + nameColumn + ")";
    boolean hasNameColumn = Arrays.asList(coreDf.columns()).contains(nameColumn);

    long candidates = coreDf.filter(functions.col(idColumn).isNotNull()).count();
    if (candidates == 0L) {
      return Optional.of(
          new JoinFunnel(label, List.of(bucket("candidates (" + idColumn + " set)", 0L))));
    }

    Column needsResolutionCond =
        hasNameColumn
            ? functions.col(idColumn).isNotNull().and(functions.col(nameColumn).isNull())
            : functions.col(idColumn).isNotNull();
    long needsResolution = coreDf.filter(needsResolutionCond).count();
    long alreadyHadName = candidates - needsResolution;

    Optional<Dataset<Row>> agentDfOpt = loader.load(TABLE_AGENT);
    boolean agentTableUsable =
        agentDfOpt.isPresent()
            && Arrays.asList(agentDfOpt.get().columns()).contains(AGENT_ID_COLUMN);

    if (!agentTableUsable) {
      return Optional.of(
          new JoinFunnel(
              label,
              List.of(
                  bucket("candidates (" + idColumn + " set)", candidates),
                  bucket("already had " + nameColumn, alreadyHadName),
                  bucket("agent table absent/unusable, left unresolved", needsResolution))));
    }

    Dataset<Row> agentIds =
        agentDfOpt
            .get()
            .select(functions.col(AGENT_ID_COLUMN).as(AGENT_JOIN_ALIAS_COLUMN))
            .distinct();
    long resolvedNewName =
        needsResolution == 0L
            ? 0L
            : coreDf
                .filter(needsResolutionCond)
                .join(
                    agentIds,
                    coreDf.col(idColumn).equalTo(agentIds.col(AGENT_JOIN_ALIAS_COLUMN)),
                    "left_semi")
                .count();
    long unresolvedNoMatch = needsResolution - resolvedNewName;

    return Optional.of(
        new JoinFunnel(
            label,
            List.of(
                bucket("candidates (" + idColumn + " set)", candidates),
                bucket("already had " + nameColumn, alreadyHadName),
                bucket("resolved, filled " + nameColumn, resolvedNewName),
                bucket("no matching agentID, unresolved", unresolvedNoMatch))));
  }

  private static JoinFunnel.Bucket bucket(String name, long count) {
    return new JoinFunnel.Bucket(name, count);
  }

  /** Quotes a Spark column identifier so qualified term URIs remain a single field name. */
  private static String quotedColumn(String columnName) {
    return "`" + columnName.replace("`", "``") + "`";
  }
}
