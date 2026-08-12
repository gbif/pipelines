package org.gbif.pipelines.spark.dwcdp.builder.extension;

import java.util.Arrays;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.pipelines.spark.util.DatasetJoins;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Builds the Humboldt Ecological Inventory Extension Dataset for the event-core path.
 *
 * <p><b>Joins:</b>
 *
 * <ul>
 *   <li>survey.event_fk = event.event_pk (left outer) → resolved to natural eventID
 *   <li>survey-survey-target / survey-target (junction) → fanned out, one row per linked target;
 *       surveys with no target produce a single row
 * </ul>
 *
 * <p>{@code samplingProtocol} → {@code eco:protocolDescriptions}; {@code samplingEffortProtocol} →
 * {@code eco:samplingEffortProtocol}.
 *
 * <p><b>Deferred:</b> {@code survey-agent-role}, {@code survey-assertion}, {@code
 * survey-identifier}, {@code survey-reference}. See mapping doc §4.10.
 */
@Slf4j
public class HumboldtExtensionBuilder {

  static final String TABLE_SURVEY = "survey";
  static final String TABLE_SURVEY_SURVEY_TARGET = "survey-survey-target";
  static final String TABLE_SURVEY_TARGET = "survey-target";

  // Derived from the real gbif-api enum rather than a hardcoded literal: this is the same value
  // HumboldtTransform.hasExtension(source, Extension.HUMBOLDT) checks against downstream, so
  // there's no longer a second copy of this URI that could silently drift out of sync — if this
  // ever changes upstream, both sides move together automatically.
  public static final String ROW_TYPE_HUMBOLDT = Extension.HUMBOLDT.getRowType();
  public static final String COL_HUMBOLDT_EXT_JSON = "humboldtExtJson";

  private HumboldtExtensionBuilder() {}

  /** Empty if survey is absent. */
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

    if (!Arrays.asList(eventDfOpt.get().columns()).contains("eventID")) {
      log.warn("event table has no eventID column; skipping Humboldt extension");
      return Optional.empty();
    }

    Dataset<Row> surveyDf = surveyDfOpt.get();
    Dataset<Row> eventDf = eventDfOpt.get();

    // "samplingProtocol" would otherwise resolve to dwc:samplingProtocol, which is not a term
    // consumed by the Humboldt interpreter. Keep publisher text where supplied and use the
    // linked protocol description only as a fallback.
    surveyDf =
        DatasetJoins.renameIfPresent(
            surveyDf, "samplingProtocol", EcoTerm.protocolDescriptions.qualifiedName());
    surveyDf =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            loader, surveyDf, "samplingProtocol_fk", EcoTerm.protocolDescriptions.qualifiedName());
    surveyDf =
        DatasetJoins.renameIfPresent(
            surveyDf, "samplingEffortProtocol", EcoTerm.samplingEffortProtocol.qualifiedName());
    surveyDf =
        ProtocolJoinBuilder.resolveProtocolFkCoalesceInto(
            loader,
            surveyDf,
            "samplingEffortProtocol_fk",
            EcoTerm.samplingEffortProtocol.qualifiedName());

    // Resolve event_fk → natural eventID; keep survey_pk for the survey-target join below
    Dataset<Row> df =
        surveyDf
            .join(
                eventDf.select("event_pk", "eventID"),
                surveyDf.col("event_fk").equalTo(eventDf.col("event_pk")),
                "left_outer")
            .drop(eventDf.col("event_pk"))
            .drop(surveyDf.col("event_fk"));

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
