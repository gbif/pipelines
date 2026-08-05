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
 * Resolves {@code agent.agentID}-referencing surrogate FK columns (e.g. {@code recordedByID},
 * {@code georeferencedByID}, {@code collectedByID}) to the linked agent's {@code
 * preferredAgentName}, coalescing the result into an existing paired free-text field (e.g. {@code
 * recordedBy}, {@code georeferencedBy}, {@code collectedBy}) — only where that field is currently
 * null, so publisher-supplied free text always wins.
 *
 * <p>Unlike {@link ProtocolJoinBuilder}, which resolves and then discards its surrogate FK column
 * (the FK itself carries no DwC term of its own), the {@code *ID} agent columns handled here
 * <em>are</em> legitimate DwC terms in their own right (e.g. {@code dwc:recordedByID}) and must
 * survive into {@code coreTerms}/extension rows unchanged — this builder only ever adds or fills
 * the paired name column, never touches the ID column itself.
 *
 * <p>Scope: this covers only the explicit {@code *ID}/{@code *By}-style field pairs already present
 * on the core tables. Per the DwC-DP data mapping guide: "Some Agent roles are explicit in DwC-DP.
 * Most of these are simply fields for the name of the Agent fulfilling the role (e.g.,
 * georeferencedBy), while others are fields for an identifier for the Agent (e.g., recordedByID).
 * Separate AgentRole records are not necessary for these explicit relationships." The {@code
 * *-agent-role} junction tables (roles with no dedicated DwC-A field, e.g. photographer,
 * preparator) are a separate, deliberately out-of-scope concern for this builder — there is no
 * confirmed DwC-A home for them the way there is for {@code recordedBy}/{@code georeferencedBy}.
 *
 * <p>Also unlike {@code protocol_fk}/{@code protocol_pk} (both internal surrogate keys), the agent
 * reference here is against {@code agent.agentID} — a natural, publisher-supplied identifier, not
 * {@code agent.agent_pk}. {@code agentID} carries no {@code uniq} constraint in the DwC-DP profile
 * ({@link #AGENT_ID_COLUMN} is the table's {@code weakPk}, not its {@code pk}), so a package
 * publishing duplicate {@code agentID}s would fan out this join; this builder does not attempt to
 * deduplicate that case — the same trust-the-profile stance {@link ProtocolJoinBuilder} takes for
 * its own required+unique surrogate keys.
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
   * Resolves {@code idColumn} (e.g. {@code "recordedByID"}) against the {@code agent} table and
   * coalesces the result into {@code nameColumn} (e.g. {@code "recordedBy"}) — only where {@code
   * nameColumn} is currently null. Creates {@code nameColumn} if it isn't already present on {@code
   * df}. {@code idColumn} itself is always left untouched.
   *
   * <p>Returns {@code df} unchanged if it has no {@code idColumn} column, or if the {@code agent}
   * table is absent or missing {@code agentID} — logged at debug/warn respectively, never thrown,
   * matching {@link ProtocolJoinBuilder}'s never-drop-the-dataset policy for optional enrichments.
   *
   * @param loader table loader — returns {@link Optional#empty()} when {@code agent} is absent
   * @param df the Dataset to resolve the FK on (event, occurrence, material, ...)
   * @param idColumn the agent-referencing FK column to resolve, e.g. {@code "recordedByID"}
   * @param nameColumn the paired free-text column to fill, e.g. {@code "recordedBy"}
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

  /** Quotes a Spark column identifier so qualified term URIs remain a single field name. */
  private static String quotedColumn(String columnName) {
    return "`" + columnName.replace("`", "``") + "`";
  }
}
