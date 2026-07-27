package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds the Humboldt Ecological Inventory Extension Dataset for the event-core path.
 *
 * <p>DwC-DP expresses survey-level inventory metadata via a {@code survey} table that links to the
 * {@code event} table via a surrogate FK ({@code event_fk → event_pk}). This builder resolves that
 * FK to the natural {@code eventID}, optionally fans out to per-survey-target rows via the {@code
 * survey-survey-target} and {@code survey-target} junction tables, and aggregates all rows per
 * {@code eventID} into a JSON column suitable for the Humboldt Extension.
 *
 * <p>Join strategy:
 *
 * <ol>
 *   <li>{@code survey.event_fk} is resolved to the natural {@code eventID} via the {@code event}
 *       table ({@code event_fk → event_pk → eventID}).
 *   <li>When both {@code survey-survey-target} and {@code survey-target} are present, the survey
 *       rows are fanned out — one output row per linked survey-target — so each survey-target's
 *       fields ({@code surveyTargetDescription}, etc.) appear as a separate Humboldt extension row
 *       under the same {@code eventID}. Surveys with no survey-target produce a single row.
 * </ol>
 *
 * <p>All internal PK/FK columns ({@code survey_pk}, {@code event_fk}, {@code survey_fk}, {@code
 * surveyTarget_fk}, {@code surveyTarget_pk}, {@code samplingProtocol_fk}, {@code
 * samplingEffortProtocol_fk}) are dropped before serialisation. Survey field names match Humboldt
 * Extension term names directly so no column renaming is needed. Returns {@link Optional#empty()}
 * if the {@code survey} table is absent.
 */
@Slf4j
public class HumboldtExtensionBuilder {

  static final String TABLE_SURVEY = "survey";
  static final String TABLE_SURVEY_SURVEY_TARGET = "survey-survey-target";
  static final String TABLE_SURVEY_TARGET = "survey-target";

  public static final String ROW_TYPE_HUMBOLDT = "http://rs.tdwg.org/eco/terms/Event";
  public static final String COL_HUMBOLDT_EXT_JSON = "humboldtExtJson";

  private HumboldtExtensionBuilder() {}

  /**
   * Builds the Humboldt extension Dataset.
   *
   * @param spark active SparkSession (needed by {@link ExtensionAggregator})
   * @param loader table loader — returns {@link Optional#empty()} when a table is absent
   * @return two-column Dataset {@code (eventID, humboldtExtJson)}, or empty if the survey table is
   *     absent
   */
  public static Optional<Dataset<Row>> build(SparkSession spark, TableLoader loader) {
    Optional<Dataset<Row>> surveyDfOpt = loader.load(TABLE_SURVEY);
    Optional<Dataset<Row>> eventDfOpt = loader.load("event");

    if (surveyDfOpt.isEmpty() || eventDfOpt.isEmpty()) {
      log.debug(
          "Skipping Humboldt extension: survey present={}, event present={}",
          surveyDfOpt.isPresent(),
          eventDfOpt.isPresent());
      return Optional.empty();
    }

    Dataset<Row> surveyDf = surveyDfOpt.get();
    Dataset<Row> eventDf = eventDfOpt.get();

    // Resolve event_fk → natural eventID; keep survey_pk for the survey-target join below
    Dataset<Row> df =
        surveyDf
            .join(
                eventDf.select("event_pk", "eventID"),
                surveyDf.col("event_fk").equalTo(eventDf.col("event_pk")),
                "left_outer")
            .drop(eventDf.col("event_pk"))
            .drop(surveyDf.col("event_fk"))
            .drop("samplingProtocol_fk")
            .drop("samplingEffortProtocol_fk");

    // Fan-out to survey-target rows via the junction table (1:many per survey)
    Optional<Dataset<Row>> junctionDfOpt = loader.load(TABLE_SURVEY_SURVEY_TARGET);
    Optional<Dataset<Row>> targetDfOpt = loader.load(TABLE_SURVEY_TARGET);

    if (junctionDfOpt.isPresent() && targetDfOpt.isPresent()) {
      Dataset<Row> junctionDf = junctionDfOpt.get();
      Dataset<Row> targetDf = targetDfOpt.get();

      Dataset<Row> targets =
          junctionDf
              .join(
                  targetDf,
                  junctionDf.col("surveyTarget_fk").equalTo(targetDf.col("surveyTarget_pk")),
                  "inner")
              .drop(targetDf.col("surveyTarget_pk"))
              .drop(junctionDf.col("surveyTarget_fk"));

      df =
          df.join(targets, df.col("survey_pk").equalTo(targets.col("survey_fk")), "left_outer")
              .drop(targets.col("survey_fk"));
    }

    df = df.drop("survey_pk");

    return Optional.of(
        ExtensionAggregator.aggregateAsJsonByKey(
            spark, df, df.columns(), "eventID", COL_HUMBOLDT_EXT_JSON));
  }
}
