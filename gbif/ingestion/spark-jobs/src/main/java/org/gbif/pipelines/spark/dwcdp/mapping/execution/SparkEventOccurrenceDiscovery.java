package org.gbif.pipelines.spark.dwcdp.mapping.execution;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.first;
import static org.apache.spark.sql.functions.lit;

import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventOccurrenceDiscoveryFragment;
import org.gbif.pipelines.spark.dwcdp.mapping.config.EventOccurrenceDiscoveryMapping;
import org.gbif.pipelines.spark.dwcdp.mapping.schema.SchemaGraph;
import org.gbif.pipelines.spark.util.TableLoader;

/**
 * Executes independent Event -> Occurrence discovery paths and reduces them to identity relations.
 */
final class SparkEventOccurrenceDiscovery {
  static final String COL_EVENT_PK = "__dwcdp_event_pk";
  static final String COL_OCCURRENCE_PK = "__dwcdp_occurrence_pk";
  static final String COL_MATERIAL_PK = "__dwcdp_material_pk";

  private final SparkSchemaPathExecutor pathExecutor;
  private final List<EventOccurrenceDiscoveryFragment> fragments;

  SparkEventOccurrenceDiscovery(SchemaGraph graph) {
    this.pathExecutor = new SparkSchemaPathExecutor(graph);
    this.fragments = EventOccurrenceDiscoveryMapping.fragments(graph);
  }

  Result discover(TableLoader loader) {
    Dataset<Row> discovered = null;
    for (EventOccurrenceDiscoveryFragment fragment : fragments) {
      if (!isExecutable(loader, fragment)) {
        continue;
      }
      SparkPathResult result = pathExecutor.execute(loader, fragment.path().schemaPath());
      Dataset<Row> branch =
          result
              .dataset()
              .select(
                  result.column(fragment.event()).cast("string").as(COL_EVENT_PK),
                  result.column(fragment.occurrence()).cast("string").as(COL_OCCURRENCE_PK),
                  fragment
                      .material()
                      .map(result::columnOrNull)
                      .orElse(lit(null))
                      .cast("string")
                      .as(COL_MATERIAL_PK))
              .filter(col(COL_EVENT_PK).isNotNull().and(col(COL_OCCURRENCE_PK).isNotNull()));
      discovered = discovered == null ? branch : discovered.unionByName(branch);
    }

    if (discovered == null) {
      throw new IllegalStateException("Event occurrence discovery has no configured fragments");
    }

    Dataset<Row> identities = discovered.distinct();
    Dataset<Row> ownership = identities.select(COL_EVENT_PK, COL_OCCURRENCE_PK).distinct();
    Dataset<Row> materialContext =
        identities
            .filter(col(COL_MATERIAL_PK).isNotNull())
            .select(COL_EVENT_PK, COL_OCCURRENCE_PK, COL_MATERIAL_PK)
            .distinct();
    Dataset<Row> uniqueMaterialContext =
        materialContext
            .groupBy(COL_EVENT_PK, COL_OCCURRENCE_PK)
            .agg(
                countDistinct(col(COL_MATERIAL_PK)).as("__dwcdp_material_count"),
                first(col(COL_MATERIAL_PK), true).as(COL_MATERIAL_PK))
            .filter(col("__dwcdp_material_count").equalTo(1))
            .drop("__dwcdp_material_count");

    return new Result(ownership, materialContext, uniqueMaterialContext);
  }

  private boolean isExecutable(TableLoader loader, EventOccurrenceDiscoveryFragment fragment) {
    if (loader.load(fragment.path().rootResource()).isEmpty()) {
      return false;
    }

    return fragment.path().schemaPath().relations().stream()
        .allMatch(
            relation ->
                loader
                        .load(relation.sourceResource())
                        .filter(dataset -> hasColumn(dataset, relation.sourceColumn()))
                        .isPresent()
                    && loader
                        .load(relation.targetResource())
                        .filter(dataset -> hasColumn(dataset, relation.targetColumn()))
                        .isPresent());
  }

  private static boolean hasColumn(Dataset<Row> dataset, String column) {
    return java.util.Arrays.asList(dataset.columns()).contains(column);
  }

  record Result(
      Dataset<Row> ownership, Dataset<Row> materialContext, Dataset<Row> uniqueMaterialContext) {}
}
